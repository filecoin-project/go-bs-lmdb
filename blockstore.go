package lmdbbs

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"

	"github.com/bmatsuo/lmdb-go/lmdb"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
)

const (
	MaxDBs        = 1       // needs to be configurable.
	MapSize       = 1 << 38 // 256GiB, this will need to be configurable.
	MaxReaders    = 128
	FreelistReuse = uint(1000) // pages, in case we decide to use https://github.com/ledgerwatch/lmdb-go (non-viral license).
)

type Blockstore struct {
	env *lmdb.Env
	db  lmdb.DBI

	closed int32
}

var (
	_ blockstore.Blockstore = (*Blockstore)(nil)
	_ blockstore.Viewer     = (*Blockstore)(nil)
)

func Open(path string) (*Blockstore, error) {
	env, err := lmdb.NewEnv()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize LMDB env: %w", err)
	}
	if err = env.SetMapSize(MapSize); err != nil {
		return nil, fmt.Errorf("failed to set LMDB map size: %w", err)
	}
	if err = env.SetMaxDBs(MaxDBs); err != nil {
		return nil, fmt.Errorf("failed to set LMDB max dbs: %w", err)
	}
	if err = env.SetMaxReaders(MaxReaders); err != nil {
		return nil, fmt.Errorf("failed to set LMDB max readers: %w", err)
	}

	if st, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.MkdirAll(path, 0777); err != nil {
			return nil, fmt.Errorf("failed to create lmdb data directory at %s: %w", path, err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("failed to check if lmdb data dir exists: %w", err)
	} else if !st.IsDir() {
		return nil, fmt.Errorf("lmdb path is not a directory %s", path)
	}

	err = env.Open(path, lmdb.NoSync|lmdb.WriteMap|lmdb.MapAsync|lmdb.NoReadahead, 0777)
	if err != nil {
		return nil, fmt.Errorf("failed to open lmdb database: %w", err)
	}

	bs := new(Blockstore)
	bs.env = env
	err = env.Update(func(txn *lmdb.Txn) (err error) {
		bs.db, err = txn.CreateDBI("blocks")
		return err
	})
	return bs, err
}

func (b *Blockstore) Close() error {
	// lmdb doesn't do close idempotency, so we only process a single
	// call to Close.
	if atomic.CompareAndSwapInt32(&b.closed, 0, 1) {
		b.env.CloseDBI(b.db)
		return b.env.Close()
	}
	return nil
}

func (b *Blockstore) Has(cid cid.Cid) (bool, error) {
	var exists bool
	err := b.env.View(func(txn *lmdb.Txn) error {
		txn.RawRead = true
		_, err := txn.Get(b.db, cid.Hash())
		if err == nil {
			exists = true
		}
		return err
	})
	if lmdb.IsNotFound(err) {
		return exists, nil
	}
	return exists, err
}

func (b *Blockstore) Get(cid cid.Cid) (blocks.Block, error) {
	var val []byte
	err := b.env.View(func(txn *lmdb.Txn) error {
		txn.RawRead = true
		v, err := txn.Get(b.db, cid.Hash())
		if err == nil {
			val = make([]byte, len(v))
			copy(val, v)
		}
		return err
	})
	if err == nil {
		return blocks.NewBlockWithCid(val, cid)
	}
	// lmdb returns badvalsize with nil keys.
	if lmdb.IsNotFound(err) || lmdb.IsErrno(err, lmdb.BadValSize) {
		return nil, blockstore.ErrNotFound
	}
	return nil, err
}

func (b *Blockstore) View(cid cid.Cid, callback func([]byte) error) error {
	err := b.env.View(func(txn *lmdb.Txn) error {
		txn.RawRead = true
		v, err := txn.Get(b.db, cid.Hash())
		if err == nil {
			return callback(v)
		}
		return err
	})
	// lmdb returns badvalsize with nil keys.
	if lmdb.IsNotFound(err) || lmdb.IsErrno(err, lmdb.BadValSize) {
		return blockstore.ErrNotFound
	}
	return err
}

func (b *Blockstore) GetSize(cid cid.Cid) (int, error) {
	size := -1
	err := b.env.View(func(txn *lmdb.Txn) error {
		txn.RawRead = true
		v, err := txn.Get(b.db, cid.Hash())
		if err == nil {
			size = len(v)
		}
		return err
	})
	if lmdb.IsNotFound(err) {
		err = blockstore.ErrNotFound
	}
	return size, err
}

func (b *Blockstore) Put(block blocks.Block) error {
	return b.env.Update(func(txn *lmdb.Txn) error {
		err := txn.Put(b.db, block.Cid().Hash(), block.RawData(), lmdb.NoOverwrite)
		if err != nil && !lmdb.IsErrno(err, lmdb.KeyExist) {
			return err
		}
		return err
	})
}

func (b *Blockstore) PutMany(blocks []blocks.Block) error {
	return b.env.Update(func(txn *lmdb.Txn) error {
		for _, block := range blocks {
			err := txn.Put(b.db, block.Cid().Hash(), block.RawData(), lmdb.NoOverwrite)
			if err != nil && !lmdb.IsErrno(err, lmdb.KeyExist) {
				return err
			}
		}
		return nil
	})
}

func (b *Blockstore) DeleteBlock(cid cid.Cid) error {
	err := b.env.Update(func(txn *lmdb.Txn) error {
		return txn.Del(b.db, cid.Hash(), nil)
	})
	if lmdb.IsNotFound(err) {
		err = nil
	}
	return err
}

func (b *Blockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	ch := make(chan cid.Cid)
	go func() {
		_ = b.env.View(func(txn *lmdb.Txn) error {
			defer close(ch)

			txn.RawRead = true
			cur, err := txn.OpenCursor(b.db)
			if err != nil {
				return err
			}
			defer cur.Close()

			for {
				if ctx.Err() != nil {
					return nil // context has fired.
				}
				k, _, err := cur.Get(nil, nil, lmdb.Next)
				if lmdb.IsNotFound(err) {
					return nil
				}
				if err != nil {
					return err
				}
				ch <- cid.NewCidV1(cid.Raw, k)
			}
		})
	}()

	return ch, nil
}

func (b *Blockstore) HashOnRead(_ bool) {
	// not supported
}
