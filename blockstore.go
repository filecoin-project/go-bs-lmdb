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
	"github.com/pkg/errors"
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

type Blockstore struct {
	// oplock is a two-tier concurrent/exclusive lock to synchronize mmap
	// growth operations. The concurrent tier is used by blockstore operations,
	// and the exclusive lock is acquired by the mmap grow operation.
	oplock sync.RWMutex

	// cursors.
	cursorlock sync.Mutex
	cursors    []*cursor

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
}

var (
	_ blockstore.Blockstore = (*Blockstore)(nil)
	_ blockstore.Viewer     = (*Blockstore)(nil)
)

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
}

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

	// Environment options:
	// --------------------
	//
	// - MDB_FIXEDMAP: Use a fixed address for the mmap region. This flag must be specified when creating the environment,
	//   and is stored persistently in the environment. If successful, the memory map will always reside at the same
	//   virtual address and pointers used to reference data items in the database will be constant across multiple
	//   invocations. This option may not always work, depending on how the operating system has allocated memory to
	//   shared libraries and other uses. The feature is highly experimental.
	// - MDB_NOSUBDIR: By default, LMDB creates its environment in a directory whose pathname is given in path, and
	//   creates its data and lock files under that directory. With this option, path is used as-is for the database
	//   main data file. The database lock file is the path with "-lock" appended.
	// - MDB_RDONLY: Open the environment in read-only mode. No write operations will be allowed. LMDB will still
	//   modify the lock file - except on read-only filesystems, where LMDB does not use locks.
	// - MDB_WRITEMAP: Use a writeable memory map unless MDB_RDONLY is set. This is faster and uses fewer mallocs,
	//   but loses protection from application bugs like wild pointer writes and other bad updates into the database.
	//   Incompatible with nested transactions. Do not mix processes with and without MDB_WRITEMAP on the same
	//   environment. This can defeat durability (mdb_env_sync etc).
	// - MDB_NOMETASYNC: Flush system buffers to disk only once per transaction, omit the metadata flush. Defer that
	//   until the system flushes files to disk, or next non-MDB_RDONLY commit or mdb_env_sync(). This optimization
	//   maintains database integrity, but a system crash may undo the last committed transaction. I.e. it preserves
	//   the ACI (atomicity, consistency, isolation) but not D (durability) database property. This flag may be changed
	//   at any time using mdb_env_set_flags().
	// - MDB_NOSYNC: Don't flush system buffers to disk when committing a transaction. This optimization means a system
	//   crash can corrupt the database or lose the last transactions if buffers are not yet flushed to disk. The risk
	//   is governed by how often the system flushes dirty buffers to disk and how often mdb_env_sync() is called.
	//   However, if the filesystem preserves write order and the MDB_WRITEMAP flag is not used, transactions exhibit
	//   ACI (atomicity, consistency, isolation) properties and only lose D (durability). i.e. database integrity is
	//   maintained, but a system crash may undo the final transactions. Note that (MDB_NOSYNC | MDB_WRITEMAP) leaves
	//   the system with no hint for when to write transactions to disk, unless mdb_env_sync() is called.
	//   (MDB_MAPASYNC | MDB_WRITEMAP) may be preferable. This flag may be changed at any time using
	//   mdb_env_set_flags().
	// - MDB_MAPASYNC: When using MDB_WRITEMAP, use asynchronous flushes to disk. As with MDB_NOSYNC, a system crash
	//   can then corrupt the database or lose the last transactions. Calling mdb_env_sync() ensures on-disk database
	//   integrity until next commit. This flag may be changed at any time using mdb_env_set_flags().
	// - MDB_NOTLS: Don't use Thread-Local Storage. Tie reader locktable slots to MDB_txn objects instead of to threads.
	//   i.e. mdb_txn_reset() keeps the slot reseved for the MDB_txn object. A thread may use parallel read-only
	//   transactions. A read-only transaction may span threads if the user synchronizes its use. Applications that
	//   multiplex many user threads over individual OS threads need this option. Such an application must also
	//   serialize the write transactions in an OS thread, since LMDB's write locking is unaware of the user threads.
	// - MDB_NOLOCK: Don't do any locking. If concurrent access is anticipated, the caller must manage all concurrency
	//   itself. For proper operation the caller must enforce single-writer semantics, and must ensure that no readers
	//   are using old transactions while a writer is active. The simplest approach is to use an exclusive lock so that
	//   no readers may be active at all when a writer begins.
	// - MDB_NORDAHEAD: Turn off readahead. Most operating systems perform readahead on read requests by default. This
	//   option turns it off if the OS supports it. Turning it off may help random read performance when the DB is
	//   larger than RAM and system RAM is full. The option is not implemented on Windows.
	// - MDB_NOMEMINIT: Don't initialize malloc'd memory before writing to unused spaces in the data file. By default,
	//   memory for pages written to the data file is obtained using malloc. While these pages may be reused in
	//   subsequent transactions, freshly malloc'd pages will be initialized to zeroes before use. This avoids
	//   persisting leftover data from other code (that used the heap and subsequently freed the memory) into the data
	//   file. Note that many other system libraries may allocate and free memory from the heap for arbitrary uses.
	//   e.g., stdio may use the heap for file I/O buffers. This initialization step has a modest performance cost so
	//   some applications may want to disable it using this flag. This option can be a problem for applications which
	//   handle sensitive data like passwords, and it makes memory checkers like Valgrind noisy. This flag is not needed
	//   with MDB_WRITEMAP, which writes directly to the mmap instead of using malloc for pages. The initialization is
	//   also skipped if MDB_RESERVE is used; the caller is expected to overwrite all of the memory that was reserved
	//   in that case. This flag may be changed at any time using mdb_env_set_flags().
	//
	// Source: http://www.lmdb.tech/doc/group__mdb.html#ga32a193c6bf4d7d5c5d579e71f22e9340

	// Maybe consider NoTLS tradeoffs.
	// https://twitter.com/yrashk/status/838621043480748036
	// https://github.com/PumpkinDB/PumpkinDB/pull/178
	var flags uint = lmdb.NoReadahead | lmdb.WriteMap
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
	}
	err = env.Update(func(txn *lmdb.Txn) (err error) {
		bs.db, err = txn.OpenRoot(lmdb.Create)
		return err
	})
	if err != nil {
		_ = env.Close()
		return nil, fmt.Errorf("failed to create/open lmdb database: %w", err)
	}
	return bs, err
}

func (b *Blockstore) Close() error {
	if !atomic.CompareAndSwapInt32(&b.closed, 0, 1) {
		return nil
	}
	b.env.CloseDBI(b.db)
	return b.env.Close()
}

func (b *Blockstore) Has(cid cid.Cid) (bool, error) {
	b.oplock.RLock()
	defer b.oplock.RUnlock()

	err := b.env.View(func(txn *lmdb.Txn) error {
		txn.RawRead = true
		_, err := txn.Get(b.db, cid.Hash())
		return err
	})
	switch {
	case err == nil:
		return true, nil
	case lmdb.IsNotFound(err):
		return false, nil
	}
	return false, err
}

func (b *Blockstore) Get(cid cid.Cid) (blocks.Block, error) {
	b.oplock.RLock()
	defer b.oplock.RUnlock()

	var val []byte

Retry:
	err := b.env.View(func(txn *lmdb.Txn) error {
		v, err := txn.Get(b.db, cid.Hash())
		if err == nil {
			val = v
		}
		return err
	})

	switch {
	case err == nil:
		return blocks.NewBlockWithCid(val, cid)
	case lmdb.IsNotFound(err) || lmdb.IsErrno(err, lmdb.BadValSize):
		// lmdb returns badvalsize with nil keys.
		err = blockstore.ErrNotFound
	case lmdb.IsErrno(err, lmdb.ReadersFull):
		b.oplock.RUnlock() // yield.
		b.sleep("get")
		b.oplock.RLock()
		goto Retry
	}
	return nil, err
}

func (b *Blockstore) View(cid cid.Cid, callback func([]byte) error) error {
	b.oplock.RLock()
	defer b.oplock.RUnlock()

Retry:
	err := b.env.View(func(txn *lmdb.Txn) error {
		txn.RawRead = true
		v, err := txn.Get(b.db, cid.Hash())
		if err == nil {
			return callback(v)
		}
		return err
	})
	switch {
	case err == nil: // shortcircuit the happy path with no comparisons.
	case lmdb.IsNotFound(err) || lmdb.IsErrno(err, lmdb.BadValSize):
		// lmdb returns badvalsize with nil keys.
		err = blockstore.ErrNotFound
	case lmdb.IsErrno(err, lmdb.ReadersFull):
		b.oplock.RUnlock() // yield.
		b.sleep("view")
		b.oplock.RLock()
		goto Retry
	}
	return err
}

func (b *Blockstore) GetSize(cid cid.Cid) (int, error) {
	b.oplock.RLock()
	defer b.oplock.RUnlock()

Retry:
	size := -1
	err := b.env.View(func(txn *lmdb.Txn) error {
		txn.RawRead = true
		v, err := txn.Get(b.db, cid.Hash())
		if err == nil {
			size = len(v)
		}
		return err
	})

	switch {
	case err == nil: // shortcircuit happy path.
	case lmdb.IsNotFound(err) || lmdb.IsErrno(err, lmdb.BadValSize):
		err = blockstore.ErrNotFound
	case lmdb.IsErrno(err, lmdb.ReadersFull):
		b.oplock.RUnlock() // yield.
		b.sleep("get size")
		b.oplock.RLock()
		goto Retry
	}

	return size, err
}

func (b *Blockstore) Put(block blocks.Block) error {
	b.oplock.RLock()
	defer b.oplock.RUnlock()

Retry:
	err := b.env.Update(func(txn *lmdb.Txn) error {
		return txn.Put(b.db, block.Cid().Hash(), block.RawData(), lmdb.NoOverwrite)
	})

	switch {
	case err == nil || lmdb.IsErrno(err, lmdb.KeyExist): // shortcircuit happy path.
		return nil
	case lmdb.IsMapFull(err):
		o := b.dedupGrow   // take the deduplicator under the lock.
		b.oplock.RUnlock() // drop the concurrent lock.
		var err error
		o.Do(func() { err = b.grow() })
		if err != nil {
			return fmt.Errorf("lmdb put failed: %w", err)
		}
		b.oplock.RLock() // reclaim the concurrent lock.
		goto Retry
	case lmdb.IsErrno(err, lmdb.ReadersFull):
		b.oplock.RUnlock() // yield.
		b.sleep("put")
		b.oplock.RLock()
		goto Retry
	}

	return err
}

func (b *Blockstore) PutMany(blocks []blocks.Block) error {
	b.oplock.RLock()
	defer b.oplock.RUnlock()

Retry:
	err := b.env.Update(func(txn *lmdb.Txn) error {
		for _, block := range blocks {
			err := txn.Put(b.db, block.Cid().Hash(), block.RawData(), lmdb.NoOverwrite)
			if err != nil && !lmdb.IsErrno(err, lmdb.KeyExist) {
				return err // short-circuit
			}
		}
		return nil
	})

	switch {
	case err == nil: // shortcircuit happy path.
	case lmdb.IsMapFull(err):
		o := b.dedupGrow   // take the deduplicator under the lock.
		b.oplock.RUnlock() // drop the concurrent lock.
		var err error
		o.Do(func() { err = b.grow() })
		if err != nil {
			return fmt.Errorf("lmdb put many failed: %w", err)
		}
		b.oplock.RLock() // reclaim the concurrent lock.
		goto Retry
	case lmdb.IsErrno(err, lmdb.ReadersFull):
		b.oplock.RUnlock() // yield.
		b.sleep("put many")
		b.oplock.RLock()
		goto Retry
	}

	return err
}

func (b *Blockstore) DeleteBlock(cid cid.Cid) error {
	b.oplock.RLock()
	defer b.oplock.RUnlock()

Retry:
	err := b.env.Update(func(txn *lmdb.Txn) error {
		return txn.Del(b.db, cid.Hash(), nil)
	})
	switch {
	case err == nil || lmdb.IsNotFound(err): // shortcircuit happy path.
		return nil
	case lmdb.IsMapFull(err):
		o := b.dedupGrow   // take the deduplicator under the lock.
		b.oplock.RUnlock() // drop the concurrent lock.
		var err error
		o.Do(func() { err = b.grow() })
		if err != nil {
			return fmt.Errorf("lmdb delete failed: %w", err)
		}
		b.oplock.RLock() // reclaim the concurrent lock.
		goto Retry
	case lmdb.IsErrno(err, lmdb.ReadersFull):
		b.oplock.RUnlock() // yield.
		b.sleep("delete")
		b.oplock.RLock()
		goto Retry
	}
	return err
}

type cursor struct {
	ctx context.Context
	b   *Blockstore

	last        cid.Cid
	outCh       chan cid.Cid
	interruptCh chan chan struct{}

	runlk  sync.Mutex
	doneCh chan struct{}
}

var errInterrupted = errors.New("interrupted")

// interrupt interrupts a cursor, and waits until the cursor is interrupted.
func (c *cursor) interrupt() {
	ch := make(chan struct{})
	select {
	case c.interruptCh <- ch:
		<-ch
	case <-c.doneCh:
		// this cursor is already done and is no longer listening for
		// interrupt signals.
	}
}

// run runs this cursor
func (c *cursor) run() {
	c.runlk.Lock()
	defer c.runlk.Unlock()

	select {
	case <-c.doneCh:
		return // already done.
	default:
	}

Retry:
	var notifyClosed chan struct{}
	err := c.b.env.View(func(txn *lmdb.Txn) error {
		txn.RawRead = true
		cur, err := txn.OpenCursor(c.b.db)
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

		for c.ctx.Err() == nil { // context has fired.
			// yield if an interrupt has been requested.
			select {
			case notifyClosed = <-c.interruptCh:
				return errInterrupted
			default:
			}

			k, _, err := cur.Get(nil, nil, lmdb.Next)
			if lmdb.IsNotFound(err) {
				return nil
			}
			if err != nil {
				return err
			}

			it := cid.NewCidV1(cid.Raw, k) // makes a copy of k
			select {
			case c.outCh <- it:
			case notifyClosed = <-c.interruptCh:
				return errInterrupted
			case <-c.ctx.Done():
				return nil
			}
			c.last = it
		}
		return nil
	})

	if lmdb.IsErrno(err, lmdb.ReadersFull) {
		log.Warnf("cursor encountered MDB_READERS_FULL; waiting %s", c.b.retryDelay)
		time.Sleep(c.b.retryDelay)
		goto Retry
	}

	if err == errInterrupted {
		close(notifyClosed)
		return
	}

	// this cursor is finished, either in success or in error.
	close(c.doneCh)
	close(c.outCh)
	c.b.cursorlock.Lock()
	for i, other := range c.b.cursors {
		if other == c {
			c.b.cursors = append(c.b.cursors[:i], c.b.cursors[i+1:]...)
			break
		}
	}
	c.b.cursorlock.Unlock()
}

// AllKeysChan starts a cursor to return all keys from the underlying
// MDB store. The cursor could be preempted at any time by a mmap grow
// operation. When that happens, the cursor yields, and the grow operation
// resumes it after the mmap expansion is completed.
//
// Consistency is not guaranteed. That is, keys returned are not a snapshot
// taken when this method is called. The set of returned keys will vary with
// concurrent writes.
func (b *Blockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	ch := make(chan cid.Cid)

	b.cursorlock.Lock()
	defer b.cursorlock.Unlock()

	c := &cursor{
		ctx:         ctx,
		b:           b,
		outCh:       ch,
		interruptCh: make(chan chan struct{}),
		doneCh:      make(chan struct{}),
	}

	b.cursors = append(b.cursors, c)

	go c.run()

	return ch, nil
}

func (b *Blockstore) HashOnRead(_ bool) {
	// not supported
}

func (b *Blockstore) grow() error {
	b.oplock.Lock() // acquire the exclusive lock.
	defer b.oplock.Unlock()

	b.cursorlock.Lock()
	defer b.cursorlock.Unlock()

	b.dedupGrow = new(sync.Once) // recycle the sync.Once.

	// interrupt all cursors.
	for _, c := range b.cursors {
		c.interrupt() // will wait until the transaction finishes.
	}

	prev, err := b.env.Info()
	if err != nil {
		return fmt.Errorf("failed to obtain env info to grow lmdb mmap: %w", err)
	}

	// Calculate the next size using the growth step factor; round to a multiple
	// of pagesize. If the proposed growth is larger than the maximum allowable
	// step, reset to the current size + max step.
	nextSize := int64(math.Ceil(float64(prev.MapSize) * b.opts.MmapGrowthStepFactor))
	nextSize = roundup(nextSize, b.pagesize) // round to a pagesize multiple.
	if nextSize > prev.MapSize+b.opts.MmapGrowthStepMax {
		nextSize = prev.MapSize + b.opts.MmapGrowthStepMax
	}
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
