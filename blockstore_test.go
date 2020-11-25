package lmdbbs

import (
	"io/ioutil"
	"os"
	"testing"

	bstest "github.com/raulk/go-bs-tests"
)

func TestLMDBBlockstore(t *testing.T) {
	s := &bstest.Suite{
		NewBlockstore:  newBlockstore,
		OpenBlockstore: openBlockstore,
	}
	s.RunTests(t, "")
}

func newBlockstore(tb testing.TB) (bstest.Blockstore, string) {
	tb.Helper()

	path, err := ioutil.TempDir("", "")
	if err != nil {
		tb.Fatal(err)
	}

	db, err := Open(path)
	if err != nil {
		tb.Fatal(err)
	}

	tb.Cleanup(func() {
		_ = os.RemoveAll(path)
	})

	return db, path
}

func openBlockstore(tb testing.TB, path string) (bstest.Blockstore, error) {
	return Open(path)
}
