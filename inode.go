package nfs

import (
	"github.com/tchajed/goose/machine/disk"

	"github.com/tchajed/go-nfs/marshal"
)

const INODE_KIND_FREE uint64 = 0
const INODE_KIND_DIR uint64 = 1
const INODE_KIND_FILE uint64 = 2

// note that 0 is an invalid Bnum
type Bnum = uint64

// inodes fit into one block, so there are exactly
// (4096-8-8-8)/8 = 509 direct blocks
const NumDirect = (4096 - 8 - 8 - 8) / 8

type Attr struct {
	// TODO: should probably store at least some permission attributes
	IsDir bool
}

type inode struct {
	Kind   uint64
	Gen    uint64 // TODO: maintain this field
	NBytes uint64
	Direct []Bnum
}

// note that 0 is an invalid Inum
type Inum = uint64

func newInode(kind uint64) inode {
	return inode{Kind: kind, Direct: make([]Bnum, NumDirect)}
}

func encodeInode(ino inode) disk.Block {
	if len(ino.Direct) != NumDirect {
		panic("invalid inode")
	}
	enc := marshal.NewEnc()
	enc.PutInt(ino.Kind)
	enc.PutInt(ino.Gen)
	enc.PutInt(ino.NBytes)
	enc.PutInts(ino.Direct)
	return enc.Finish()
}

func decodeInode(b disk.Block) inode {
	ino := inode{}
	dec := marshal.NewDec(b)
	ino.Kind = dec.GetInt()
	ino.Gen = dec.GetInt()
	ino.NBytes = dec.GetInt()
	ino.Direct = dec.GetInts(NumDirect)
	return ino
}
