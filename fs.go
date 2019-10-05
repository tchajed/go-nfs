package nfs

import (
	"github.com/tchajed/go-awol"
	"github.com/tchajed/goose/machine/disk"

	"github.com/tchajed/go-nfs/balloc"
	"github.com/tchajed/go-nfs/marshal"
)

// file-system layout (on top of logical disk exposed by log):
// [ superblock | block bitmaps | inodes | data blocks ]

type SuperBlock struct {
	// serialized
	NumInodes       uint64
	NumBlockBitmaps uint64

	// in-memory
	blockAllocBase uint64
	inodeBase      uint64
	numInodes      uint64
	dataBase       uint64
	fsSize         uint64
}

func (sb *SuperBlock) computeFields() {
	sb.blockAllocBase = 1
	sb.inodeBase = sb.blockAllocBase + sb.NumBlockBitmaps
	sb.numInodes = sb.NumInodes
	sb.dataBase = sb.inodeBase + sb.numInodes
	sb.fsSize = sb.dataBase + balloc.ItemsPerBitmap*sb.NumBlockBitmaps
}

// x/k, rounded up
func divUp(x uint64, k uint64) uint64 {
	return (x + (k - 1)) / k
}

func NewSuperBlock(diskSize uint64) *SuperBlock {
	// this isn't the precise threshold
	if diskSize < 10 {
		panic("disk too small")
	}
	numInodes := diskSize / 4
	blockBitmaps := divUp(diskSize-1-numInodes, balloc.ItemsPerBitmap)
	sb := &SuperBlock{
		NumInodes:       numInodes,
		NumBlockBitmaps: blockBitmaps,
	}
	sb.computeFields()
	return sb
}

func encodeSuperBlock(sb *SuperBlock) disk.Block {
	enc := marshal.NewEnc()
	enc.PutInt(sb.NumInodes)
	enc.PutInt(sb.NumBlockBitmaps)
	return enc.Finish()
}

func decodeSuperBlock(b disk.Block) *SuperBlock {
	sb := new(SuperBlock)
	dec := marshal.NewDec(b)
	sb.NumInodes = dec.GetInt()
	sb.NumBlockBitmaps = dec.GetInt()
	sb.computeFields()
	return sb
}

type Fs struct {
	log    *awol.Log
	sb     *SuperBlock
	blockA balloc.Bitmap
}

func NewFs(log *awol.Log) Fs {
	sb := NewSuperBlock(uint64(log.Size()))
	blockA := balloc.Init(int(sb.NumBlockBitmaps))
	op := log.Begin()

	op.Write(0, encodeSuperBlock(sb))
	blockA.Flush(op, sb.blockAllocBase)

	freeInode := encodeInode(newInode(INODE_KIND_FREE))
	for i := Inum(1); i < sb.numInodes; i++ {
		op.Write(sb.inodeBase+(i-1), freeInode)
	}
	log.Commit(op)
	return Fs{log: log, sb: sb, blockA: blockA}
}

func OpenFs(log *awol.Log) Fs {
	sb := decodeSuperBlock(log.Read(0))
	blockA := balloc.Open(log, sb.blockAllocBase, int(sb.NumBlockBitmaps))
	return Fs{log: log, sb: sb, blockA: blockA}
}

func (fs Fs) flushBalloc(op *awol.Op) {
	fs.blockA.Flush(op, fs.sb.blockAllocBase)
}

// btoa translates an offset in an inode to a block number
//
// does not depend on the rest of the disk because there are no indirect blocks
func (ino inode) btoa(boff uint64) Bnum {
	if boff >= uint64(len(ino.Direct)) {
		panic("invalid block offset")
	}
	return ino.Direct[boff]
}

func (ino inode) readBlock(fs Fs, boff uint64) disk.Block {
	return fs.log.Read(fs.sb.inodeBase + ino.btoa(boff))
}

func (ino inode) putBlock(fs Fs, op *awol.Op, boff uint64, b disk.Block) {
	op.Write(fs.sb.inodeBase+ino.btoa(boff), b)
}

func (fs Fs) getInode(i Inum) inode {
	if i == 0 {
		panic("0 is an invalid inode number")
	}
	if i > fs.sb.numInodes {
		panic("invalid inode number")
	}
	b := fs.log.Read(fs.sb.inodeBase + (i - 1))
	return decodeInode(b)
}

func (fs Fs) findFreeInode() (Inum, inode) {
	for i := uint64(1); i <= fs.sb.numInodes; i++ {
		ino := fs.getInode(i)
		if ino.Kind == INODE_KIND_FREE {
			return i, ino
		}
	}
	return 0, inode{}
}

func (fs Fs) flushInode(op *awol.Op, i Inum, ino inode) {
	op.Write(fs.sb.inodeBase+(i-1), encodeInode(ino))
}

// returns false if grow failed (eg, due to inode size or running out of blocks)
func (fs Fs) growInode(op *awol.Op, ino inode, newLen uint64) bool {
	if !(ino.NBytes <= newLen) {
		panic("growInode requires a larger length")
	}
	oldBlks := divUp(ino.NBytes, disk.BlockSize)
	newBlks := divUp(newLen, disk.BlockSize)
	if newBlks > NumDirect {
		return false
	}
	for b := oldBlks; b < newBlks; b++ {
		newB := fs.blockA.Alloc()
		if newB >= fs.blockA.Size() {
			// TODO: this leaves the allocator and inode in an inconsistent
			//  state; it's easy to throw away the inode,
			//  but the changes to the allocator are harder to revert.
			//
			// maybe we should allocate all the blocks in the
			// allocator? or just operate on a copy?
			return false
		}
		ino.Direct[b] = newB
	}
	ino.NBytes = newLen
	// inode and block allocator are dirty
	return true
}

func (fs Fs) shrinkInode(op *awol.Op, ino inode, newLen uint64) {
	if !(newLen <= ino.NBytes) {
		panic("shrinkInode requires a smaller length")
	}
	oldBlks := divUp(ino.NBytes, disk.BlockSize)
	newBlks := divUp(newLen, disk.BlockSize)
	// newBlks <= oldBlks
	for b := newBlks; b <= oldBlks; b++ {
		oldB := ino.btoa(b)
		fs.blockA.Free(oldB)
	}
	ino.NBytes = newLen
}
