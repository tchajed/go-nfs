package nfs

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/tchajed/go-awol/mem"
)

type FsSuite struct {
	suite.Suite
	fs Fs
}

func (suite *FsSuite) SetupTest() {
	log := mem.New(10 * 1000)
	suite.fs = NewFs(log)
	//fmt.Printf("fs: %+v\n", suite.fs.sb)
}

func (suite *FsSuite) TestGetRoot() {
	fs := suite.fs
	root := fs.RootInode()
	attr, ok := fs.GetAttr(root)
	suite.Require().True(ok)
	suite.True(attr.IsDir, "root should be a directory")
}

func (suite *FsSuite) TestCreateFile() {
	fs := suite.fs
	root := fs.RootInode()
	i1, ok := fs.Create(root, "foo", false)
	suite.Require().True(ok)

	_, ok = fs.GetAttr(i1)
	suite.Require().True(ok, "created file should exist")

	i2, ok := fs.Create(root, "bar", false)
	suite.True(ok)
	if !suite.T().Failed() {
		suite.NotEqual(i1, i2)
	}
}

func (suite *FsSuite) TestCreateFiles() {
	fs := suite.fs
	root := fs.RootInode()
	i1, ok := fs.Create(root, "foo", false)
	suite.Equal(uint64(2), i1)
	suite.True(ok)

	i2, ok := fs.Create(root, "bar", false)
	suite.True(ok)
	if !suite.T().Failed() {
		suite.NotEqual(i1, i2)
	}
}

func (suite *FsSuite) TestCreateDir() {
	fs := suite.fs
	root := fs.RootInode()
	i1, ok := fs.Mkdir(root, "foo")
	suite.Require().True(ok)
	i2, ok := fs.Create(i1, "bar", false)
	suite.True(ok)
	suite.NotEqual(i1, i2)
}

// TODO: something is seriously wrong that breaks this test
//
// we don't seem to correctly allocate new directory entry blocks to directories
func (suite *FsSuite) TestUncheckedCreate() {
	fs := suite.fs
	root := fs.RootInode()
	_, ok := fs.Create(root, "foo", false)
	suite.True(ok)
	// this is checked, should fail
	_, ok = fs.Create(root, "foo", false)
	suite.False(ok)
	// this is unchecked, overwrites the previous inode
	_, ok = fs.Create(root, "foo", true)
	suite.True(ok)
}

func TestFs(t *testing.T) {
	suite.Run(t, new(FsSuite))
}
