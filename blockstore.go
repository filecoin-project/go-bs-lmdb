package lmdbbs

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ledgerwatch/lmdb-go/lmdb"
	uatomic "go.uber.org/atomic"
)

var (
	// DefaultInitialMmapSize is the default initial mmap size to be used if the
	// supplied value is zero or invalid. Unless modified, this value is 1GiB.
	DefaultInitialMmapSize = int64(1 << 30) // 1GiB.

	// DefaultMmapGrowthStepFactor is the default mmap growth step factor to be
	// used if the supplied value is zero or invalid. Unless modified, this
	// value is 1.5, which multiplies the mmap size by 1.5 every time we
	// encounter an MDB_MAP_FULL error.
	DefaultMmapGrowthStepFactor = 1.5 // 1.5x the mmap every time.

	// DefaultMmapGrowthStepMax is the default mmap growth maximum step to be
	// used if the supplied value is zero or invalid. Unless modified, this
	// value is 4GiB.
	DefaultMmapGrowthStepMax = int64(4 << 30) // maximum step size is 4GiB at a time.

	// DefaultMaxReaders is the default number of max readers if one is not
	// provided. By default it is 254, not 256, following the note from the LMDB
	// author that indicates that the original default was 126 because it fit
	// exactly into 8KiB along with a couple of mutexes.
	//
	// http://www.lmdb.tech/doc/group__readers.html#gadff1f7b4d4626610a8d616e0c6dbbea4
	DefaultMaxReaders = 254

	// DefaultRetryDelay is the default retry delay for reattempting transactions
	// that errored with MDB_READERS_FULL.
	DefaultRetryDelay = 10 * time.Millisecond

	// RetryJitter is the jitter to apply to the delay interval. Default: 20%.
	RetryJitter = 0.2
)

var log = logging.Logger("lmdbbs")

// Blockstore is a wrapper around lmdb database and environment,
// containing some additional parameters to execute growing of
// mmaped section.
type Blockstore struct {
	// oplock is a two-tier concurrent/exclusive lock to synchronize mmap
	// growth operations. The concurrent tier is used by blockstore operations,
	// and the exclusive lock is acquired by the mmap grow operation.
	oplock sync.RWMutex

	// mutex guarding all operations with cursor list
	cursorlock sync.Mutex
	// list of all active cursors
	cursors []*cursor

	// dedupGrow deduplicates concurrent calls to grow(); it is recycled every
	// time that grow() actually runs.
	dedupGrow *sync.Once

	// env represents a database environment. An lmdb env can contain multiple
	// databases, all residing in the same shared memory map, but the blockstore
	// only uses a 1:1 mapping between envs and dbs.
	env *lmdb.Env
	// db is an object representing an LMDB database inside an env.
	db lmdb.DBI
	// opts are the options for this blockstore.
	opts *Options

	retryDelay       time.Duration
	retryJitterBound time.Duration
	pagesize         int64 // the memory page size reported by the OS.
	closed           int32
	rehash           *uatomic.Bool
}

// HashOnRead sets a variable which controls whether
// cid is checked to match the hash of the block
func (b *Blockstore) HashOnRead(enabled bool) {
	b.rehash.Store(enabled)
}

var (
	_ blockstore.Blockstore = (*Blockstore)(nil)
	_ blockstore.Viewer     = (*Blockstore)(nil)
)

// Options for the Blockstore
type Options struct {
	// Path is the directory where the LMDB blockstore resides. If it doesn't
	// exist, it will be created.
	Path string

	// ReadOnly, if true, opens this blockstore in read-only mode.
	ReadOnly bool

	// NoSync disables flushing system buffers to disk immediately when
	// committing transactions.
	NoSync bool

	// +++ DB sizing fields. +++ //
	// InitialMmapSize is the initial mmap size passed to LMDB when
	// creating/opening the environment.
	InitialMmapSize int64

	// MmapGrowthStepFactor determines the next map size when a write fails. The
	// current size is multiplied by the factor, and rounded up to the next
	// value divisible by os.Getpagesize(), to obtain the new map size, which is
	// applied with mdb_env_set_mapsize.
	MmapGrowthStepFactor float64

	// MmapGrowthStepMax is the maximum step size by which we'll grow the mmap.
	MmapGrowthStepMax int64
	// --- DB sizing fields. --- //

	// MaxReaders is the maximum amount of concurrent reader slots that exist
	// in the lock table. By default 254.
	MaxReaders int

	// RetryDelay is a fixed delay to wait before a transaction that errored
	// with MDB_READERS_FULL will be reattempted. Contention due to incorrect
	// sizing of MaxReaders will thus lead to a system slowdown via
	// backpressure, instead of a straight out error.
	// Jittered by RetryJitter (default: +/-20%).
	RetryDelay time.Duration

	// NoLock: Don't do any locking. If concurrent access is anticipated, the caller must manage all concurrency
	//   itself. For proper operation the caller must enforce single-writer semantics, and must ensure that no readers
	//   are using old transactions while a writer is active. The simplest approach is to use an exclusive lock so that
	//   no readers may be active at all when a writer begins.
	NoLock bool
}

// Open initiates lmdb environment, database and returns Blockstore
func Open(opts *Options) (*Blockstore, error) {
	path := opts.Path
	switch st, err := os.Stat(path); {
	case os.IsNotExist(err):
		if err := os.MkdirAll(path, 0777); err != nil {
			return nil, fmt.Errorf("failed to create lmdb data directory at %s: %w", path, err)
		}
	case err != nil:
		return nil, fmt.Errorf("failed to check if lmdb data dir exists: %w", err)
	case !st.IsDir():
		return nil, fmt.Errorf("lmdb path is not a directory %s", path)
	}

	pagesize := os.Getpagesize()
	if pagesize < 0 {
		pagesize = 4096 // being defensive and setting a safe value (4KiB).
	}

	// Validate mmap sizing parameters.
	//
	// InitialMmapSize cannot be negative nor zero, and must be rounded up to a multiple of the OS page size.
	if v := opts.InitialMmapSize; v <= 0 {
		log.Debugf("using default initial mmap size: %d", DefaultInitialMmapSize)
		opts.InitialMmapSize = DefaultInitialMmapSize
	}
	if v := roundup(opts.InitialMmapSize, int64(pagesize)); v != opts.InitialMmapSize {
		log.Warnf("initial mmap size (%d) must be a multiple of the OS pagesize (%d); rounding up: %d", opts.InitialMmapSize, pagesize, v)
		opts.InitialMmapSize = v
	}

	// MmapGrowthStepMax cannot be negative nor zero, and must be rounded up to a multiple of the OS page size.
	if v := opts.MmapGrowthStepMax; v <= 0 {
		log.Debugf("using default max mmap growth step: %d", DefaultMmapGrowthStepMax)
		opts.MmapGrowthStepMax = DefaultMmapGrowthStepMax
	}
	if v := roundup(opts.MmapGrowthStepMax, int64(pagesize)); v != opts.MmapGrowthStepMax {
		log.Warnf("maximum mmap growth step (%d) must be a multiple of the OS pagesize (%d); rounding up: %d", opts.MmapGrowthStepMax, pagesize, v)
		opts.MmapGrowthStepMax = v
	}

	if v := opts.MmapGrowthStepFactor; v <= 1 {
		log.Debugf("using default mmap growth step factor: %f", DefaultMmapGrowthStepFactor)
		opts.MmapGrowthStepFactor = DefaultMmapGrowthStepFactor
	}

	// Create an LMDB environment. We set the initial mapsize, but do not set
	// max DBs, since a blockstore only requires a single, unnamed LMDB DB.
	// see: http://www.lmdb.tech/doc/group__mdb.html#gaa2fc2f1f37cb1115e733b62cab2fcdbc
	env, err := lmdb.NewEnv()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize LMDB env: %w", err)
	}
	if err = env.SetMapSize(opts.InitialMmapSize); err != nil {
		return nil, fmt.Errorf("failed to set LMDB map size: %w", err)
	}
	// Use the default max readers (254) unless a value is passed in the options.
	if opts.MaxReaders == 0 {
		opts.MaxReaders = DefaultMaxReaders
	}
	if err = env.SetMaxReaders(opts.MaxReaders); err != nil {
		return nil, fmt.Errorf("failed to set LMDB max readers: %w", err)
	}
	// Use a default retry delay if none is set.
	if opts.RetryDelay == 0 {
		opts.RetryDelay = DefaultRetryDelay
	}

	// See ENV options: http://www.lmdb.tech/doc/group__mdb.html#ga32a193c6bf4d7d5c5d579e71f22e9340

	// Maybe consider NoTLS tradeoffs.
	// https://twitter.com/yrashk/status/838621043480748036
	// https://github.com/PumpkinDB/PumpkinDB/pull/178
	var flags uint = lmdb.NoReadahead | lmdb.WriteMap
	if opts.NoLock {
		flags |= lmdb.NoLock
	}
	if opts.ReadOnly {
		flags |= lmdb.Readonly
	}
	if opts.NoSync {
		flags |= lmdb.MapAsync
	}
	err = env.Open(path, flags, 0777)
	if err != nil {
		return nil, fmt.Errorf("failed to open lmdb database: %w", err)
	}

	bs := &Blockstore{
		env:              env,
		opts:             opts,
		dedupGrow:        new(sync.Once),
		pagesize:         int64(pagesize),
		retryDelay:       opts.RetryDelay,
		retryJitterBound: time.Duration(float64(opts.RetryDelay) * RetryJitter),
		rehash:           uatomic.NewBool(false),
	}
	err = env.View(func(txn *lmdb.Txn) (err error) {
		bs.db, err = txn.OpenRoot(lmdb.Create)
		return err
	})
	if err != nil {
		_ = env.Close()
		return nil, fmt.Errorf("failed to create/open lmdb database: %w", err)
	}
	return bs, err
}

// Close closes the blockstore
func (b *Blockstore) Close() error {
	if !atomic.CompareAndSwapInt32(&b.closed, 0, 1) {
		return nil
	}
	b.env.CloseDBI(b.db)
	return b.env.Close()
}

func (b *Blockstore) getImpl(cid cid.Cid, handler func(val []byte) error) error {
	b.oplock.RLock()
	defer b.oplock.RUnlock()

	for {
		err := b.env.View(func(txn *lmdb.Txn) error {
			txn.RawRead = true
			v, err := txn.Get(b.db, cid.Hash())
			if err == nil {
				err = handler(v)
			}
			return err
		})
		switch {
		case lmdb.IsErrno(err, lmdb.ReadersFull):
			b.oplock.RUnlock() // yield.
			b.sleep("getImpl")
			b.oplock.RLock()
		case lmdb.IsMapResized(err):
			if err = b.growGuarded(); err != nil {
				return err
			}
		default:
			return err
		}
	}
}

func wrapGrowError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("lmdb grow failed: %w", err)
}

func (b *Blockstore) growGuarded() error {
	o := b.dedupGrow       // take the deduplicator under the lock.
	b.oplock.RUnlock()     // drop the concurrent lock.
	defer b.oplock.RLock() // reclaim the concurrent lock.
	var err error
	o.Do(func() { err = b.grow() })
	return wrapGrowError(err)
}

func (b *Blockstore) updateImpl(doUpdate func(txn *lmdb.Txn) error) error {
	b.oplock.RLock()
	defer b.oplock.RUnlock()
	for {
		err := b.env.Update(doUpdate)
		switch {
		case err == nil: // shortcircuit happy path.
			return nil
		case lmdb.IsMapFull(err) || lmdb.IsMapResized(err):
			if err = b.growGuarded(); err != nil {
				return err
			}
		case lmdb.IsErrno(err, lmdb.ReadersFull):
			b.oplock.RUnlock() // yield.
			b.sleep("updateImpl")
			b.oplock.RLock()
		default:
			return err
		}
	}
}

// Has checks if the cid is present in the blockstore
func (b *Blockstore) Has(cid cid.Cid) (bool, error) {
	err := b.getImpl(cid, func(val []byte) error { return nil })
	if lmdb.IsNotFound(err) {
		return false, nil
	}
	return err == nil, err
}

// Get retrieves a block with the given cid
// When block is not found, blockstore.ErrNotFound is returned
// Please note that bytes of the whole block are copied,
// if you want only to read some of the bytes of the block, use View
func (b *Blockstore) Get(key cid.Cid) (blocks.Block, error) {
	var res blocks.Block
	err := b.getImpl(key, func(v []byte) (err error) {
		if b.rehash.Load() {
			var key2 cid.Cid
			key2, err = key.Prefix().Sum(v)
			if err != nil && !key2.Equals(key) {
				err = blockstore.ErrHashMismatch
			}
		}
		if err != nil {
			return
		}
		val := make([]byte, len(v))
		copy(val, v)
		res, err = blocks.NewBlockWithCid(val, key)
		return
	})

	if lmdb.IsNotFound(err) || lmdb.IsErrno(err, lmdb.BadValSize) {
		// lmdb returns badvalsize with nil keys.
		return nil, blockstore.ErrNotFound
	}
	return res, err
}

// View retrieves bytes of a block with the given cid and
// calls the callback on it.
// When block is not found, blockstore.ErrNotFound is returned
// Note that it is not safe to access bytes passed to the callback
// outside of the callback's call context.
func (b *Blockstore) View(cid cid.Cid, callback func([]byte) error) error {
	err := b.getImpl(cid, callback)
	if lmdb.IsNotFound(err) || lmdb.IsErrno(err, lmdb.BadValSize) {
		// lmdb returns badvalsize with nil keys.
		return blockstore.ErrNotFound
	}
	return err
}

// GetSize returns size of the block.
// When block is not found, blockstore.ErrNotFound is returned
func (b *Blockstore) GetSize(cid cid.Cid) (int, error) {
	size := -1
	err := b.getImpl(cid, func(v []byte) error {
		size = len(v)
		return nil
	})
	if lmdb.IsNotFound(err) || lmdb.IsErrno(err, lmdb.BadValSize) {
		// lmdb returns badvalsize with nil keys.
		err = blockstore.ErrNotFound
	}
	return size, err
}

// Put adds the block to the blockstore
// This is a no-op when block is already present in the Blockstore,
// no overwrite will take place.
func (b *Blockstore) Put(block blocks.Block) error {
	return b.updateImpl(func(txn *lmdb.Txn) error {
		err := txn.Put(b.db, block.Cid().Hash(), block.RawData(), lmdb.NoOverwrite)
		if err == nil || lmdb.IsErrno(err, lmdb.KeyExist) {
			return nil
		}
		return err
	})
}

// PutMany adds the blocks to the blockstore
// This is a no-op for blocks that are already present in the Blockstore,
// no overwrites will take place.
func (b *Blockstore) PutMany(blocks []blocks.Block) error {
	return b.updateImpl(func(txn *lmdb.Txn) error {
		for _, block := range blocks {
			err := txn.Put(b.db, block.Cid().Hash(), block.RawData(), lmdb.NoOverwrite)
			if err != nil && !lmdb.IsErrno(err, lmdb.KeyExist) {
				return err // short-circuit
			}
		}
		return nil
	})
}

// DeleteBlock removes the block from the blockstore, given its cid.
// This is a no-op for cid that is absent in the Blockstore.
func (b *Blockstore) DeleteBlock(cid cid.Cid) error {
	return b.updateImpl(func(txn *lmdb.Txn) error {
		err := txn.Del(b.db, cid.Hash(), nil)
		if err == nil || lmdb.IsNotFound(err) {
			return nil
		}
		return err
	})
}

// DeleteMany removes blocks from the blockstore with the given cids.
// This is a no-op for cids that are absent in the Blockstore.
func (b *Blockstore) DeleteMany(cids []cid.Cid) error {
	return b.updateImpl(func(txn *lmdb.Txn) error {
		for _, c := range cids {
			err := txn.Del(b.db, c.Hash(), nil)
			if err != nil && !lmdb.IsNotFound(err) {
				return err
			}
		}
		return nil
	})
}

type cursor struct {
	// Context to finish iteration when it is finished
	ctx context.Context
	// blockstore which we should update when iteration is finished
	blockstore *Blockstore

	// Resulting channel with all the keys
	out chan cid.Cid

	// last cid that was sent on the `out`
	// used to restart iteration properly after
	// interruption followed by grow() operation
	last cid.Cid

	// structure to interrupt the cursor
	interrupt interruptChan
}

// run runs this cursor
func (c *cursor) run() {
	if c.interrupt.IsClosed() {
		return
	}

	for {
		var notifyClosed chan struct{}
		err := c.blockstore.env.View(func(txn *lmdb.Txn) error {
			txn.RawRead = true
			cur, err := txn.OpenCursor(c.blockstore.db)
			if err != nil {
				return err
			}
			defer cur.Close()

			if c.last.Defined() {
				_, _, err := cur.Get(c.last.Hash(), nil, lmdb.Set)
				if err != nil {
					return fmt.Errorf("failed to position cursor: %w", err)
				}
			}

			for c.ctx.Err() == nil { // context is not done
				// yield if an interrupt has been requested.
				notifyClosed = c.interrupt.IsInterrupted()
				if notifyClosed != nil {
					return nil
				}

				k, _, err := cur.Get(nil, nil, lmdb.Next)
				if lmdb.IsNotFound(err) {
					return nil
				} else if err != nil {
					return err
				}

				it := cid.NewCidV1(cid.Raw, k) // makes a copy of k
				select {
				case c.out <- it:
				case notifyClosed = <-c.interrupt.InterruptChan():
					// return nil if there was an interrupt
					return nil
				case <-c.ctx.Done():
					return nil
				}
				c.last = it
			}
			return nil
		})

		if lmdb.IsErrno(err, lmdb.ReadersFull) {
			log.Warnf("cursor encountered MDB_READERS_FULL; waiting %s", c.blockstore.retryDelay)
			time.Sleep(c.blockstore.retryDelay)
		} else if notifyClosed != nil {
			close(notifyClosed)
			return
		} else {
			// this cursor is finished, either in success or in error.
			c.interrupt.Close()
			close(c.out)
			c.blockstore.cursorlock.Lock()
			defer c.blockstore.cursorlock.Unlock()
			for i, other := range c.blockstore.cursors {
				if other == c {
					c.blockstore.cursors = append(c.blockstore.cursors[:i], c.blockstore.cursors[i+1:]...)
					break
				}
			}
			return
		}
	}
}

// AllKeysChan starts a cursor to return all keys from the underlying
// MDB store. The cursor could be preempted at any time by a mmap grow
// operation. When that happens, the cursor yields, and the grow operation
// resumes it after the mmap expansion is completed.
//
// Consistency is not guaranteed. That is, keys returned are not a snapshot
// taken when this method is called. The set of returned keys will vary with
// concurrent writes.
//
// All errors happening while iterating (except for concurrent reader limit hit)
// are ignored, hence this method is to be avoided whenever possible.
func (b *Blockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	ch := make(chan cid.Cid)

	b.cursorlock.Lock()
	defer b.cursorlock.Unlock()

	c := &cursor{
		ctx:        ctx,
		blockstore: b,
		out:        ch,
		interrupt:  makeInterruptChan(),
	}

	b.cursors = append(b.cursors, c)

	go c.run()

	return ch, nil
}

func (b *Blockstore) grow() error {
	// acquire the exclusive lock so that no new update
	// or read operation will start while grow() runs
	// (already running operations will finish first)
	b.oplock.Lock()
	defer b.oplock.Unlock()

	// acquire cursor lock so that no cursors will
	// be created or finished while grow() runs
	b.cursorlock.Lock()
	defer b.cursorlock.Unlock()

	b.dedupGrow = new(sync.Once) // recycle the sync.Once.

	// interrupt all cursors.
	for _, c := range b.cursors {
		// will wait until cursor's run() finishes
		c.interrupt.Interrupt()
	}

	prev, err := b.env.Info()
	if err != nil {
		return fmt.Errorf("failed to obtain env info to grow lmdb mmap: %w", err)
	}

	// Calculate the next size using the growth step factor; round to a multiple
	// of pagesize. If the proposed growth is larger than the maximum allowable
	// step, reset to the current size + max step.
	nextSize := int64(math.Ceil(float64(prev.MapSize) * b.opts.MmapGrowthStepFactor))
	if nextSize > prev.MapSize+b.opts.MmapGrowthStepMax {
		nextSize = prev.MapSize + b.opts.MmapGrowthStepMax
	}
	nextSize = roundup(nextSize, b.pagesize) // round to a pagesize multiple.
	if err := b.env.SetMapSize(nextSize); err != nil {
		return fmt.Errorf("failed to grow the mmap: %w", err)
	}
	next, err := b.env.Info()
	if err != nil {
		return fmt.Errorf("failed to obtain env info after growing lmdb mmap: %w", err)
	}
	log.Infof("grew lmdb mmap: %d => %d", prev.MapSize, next.MapSize)

	// resume all cursors.
	for _, c := range b.cursors {
		go c.run()
	}
	return nil
}

func (b *Blockstore) sleep(opname string) {
	r := rand.Int63n(int64(b.retryJitterBound))
	// we don't need this to be perfect, we need it to be performant,
	// so we add when even, remove when odd.
	if r%2 == 1 {
		r = -r
	}
	d := b.retryDelay + time.Duration(r)
	log.Warnf(opname+" encountered MDB_READERS_FULL; waiting %s", d)
	time.Sleep(d)
}

func roundup(value, multiple int64) int64 {
	return int64(math.Ceil(float64(value)/float64(multiple))) * multiple
}

// Stat returns lmdb statistics
func (b *Blockstore) Stat() (*lmdb.Stat, error) {
	return b.env.Stat()
}
