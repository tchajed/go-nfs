package balloc

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/tchajed/go-awol"
	"github.com/tchajed/goose/machine/disk"
)

type BallocSuite struct {
	suite.Suite
	log *awol.Log
}

func (suite *BallocSuite) fresh(alloc Bitmap) uint64 {
	suite.T().Helper()
	key, ok := alloc.Alloc()
	suite.Require().True(ok)
	return key
}

func (suite *BallocSuite) full(alloc Bitmap) {
	suite.T().Helper()
	key, ok := alloc.Alloc()
	suite.False(ok, "allocator should be full but allocated %d", key)
}

func (suite *BallocSuite) SetupTest() {
	disk.Init(disk.NewMemDisk(1000))
	if disk.Size() < 10 {
		panic("disk too small")
	}
	suite.log = awol.New()
}

func (suite *BallocSuite) TestAlloc2() {
	alloc := Init(3)
	key1 := suite.fresh(alloc)
	key2 := suite.fresh(alloc)
	suite.NotEqual(key2, key1)
}

func (suite *BallocSuite) TestAllocAll() {
	alloc := Init(3)
	for i := 0; i < 3*ItemsPerBitmap; i++ {
		suite.fresh(alloc)
	}
	suite.full(alloc)
	suite.full(alloc)
}

func (suite *BallocSuite) TestFree() {
	alloc := Init(3)
	key1 := suite.fresh(alloc)
	key2 := suite.fresh(alloc)
	alloc.Free(key1)
	alloc.Free(key2)
	for i := 0; i < 3*ItemsPerBitmap; i++ {
		suite.fresh(alloc)
	}
}

func (suite *BallocSuite) TestFlushReopen() {
	alloc := Init(3)
	for i := 0; i < ItemsPerBitmap; i++ {
		suite.fresh(alloc)
	}
	alloc.Free(10)
	alloc.Free(22)
	op := suite.log.Begin()
	alloc.Flush(op, 1)
	suite.log.Commit(op)
	alloc = Open(suite.log, 1, 3)
	// this is the only test that assumes identifiers are returned in order
	suite.Equal(uint64(10), suite.fresh(alloc))
	suite.Equal(uint64(22), suite.fresh(alloc))
	alloc.Free(1000)
}

func BenchmarkAllocFree(b *testing.B) {
	for benchIter := 0; benchIter < b.N; benchIter++ {
		alloc := Init(3)
		for i := 0; i < 3*ItemsPerBitmap; i++ {
			alloc.Alloc()
		}
		numFreed := 0
		for i := uint64(0); i < 3*ItemsPerBitmap; i += 3 {
			alloc.Free(i)
			numFreed++
		}
		for i := 0; i < numFreed; i++ {
			alloc.Alloc()
		}
	}
}

func TestBalloc(t *testing.T) {
	suite.Run(t, new(BallocSuite))
}
