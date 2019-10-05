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
	rootInode      Inum
	inodeBase      uint64
	numInodes      uint64
	dataBase       uint64
	fsSize         uint64
}

func (sb *SuperBlock) computeFields() {
	sb.blockAllocBase = 1
	sb.rootInode = 1
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
	numInodes := (diskSize - 1 - 1) / 4
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
	log *awol.Log
	sb  *SuperBlock
}

func NewFs(log *awol.Log) Fs {
	sb := NewSuperBlock(uint64(log.Size()))
	blockA := balloc.Init(int(sb.NumBlockBitmaps))
	op := log.Begin()

	op.Write(0, encodeSuperBlock(sb))
	blockA.Flush(op, sb.blockAllocBase)

	op.Write(sb.inodeBase+(sb.rootInode-1),
		encodeInode(newInode(INODE_KIND_DIR)))
	freeInode := encodeInode(newInode(INODE_KIND_FREE))
	for i := Inum(2); i < sb.numInodes; i++ {
		op.Write(sb.inodeBase+(i-1), freeInode)
	}
	log.Commit(op)
	return Fs{log: log, sb: sb}
}

func OpenFs(log *awol.Log) Fs {
	sb := decodeSuperBlock(log.Read(0))
	return Fs{log: log, sb: sb}
}

func (fs Fs) readBalloc() balloc.Bitmap {
	return balloc.Open(fs.log,
		fs.sb.blockAllocBase,
		int(fs.sb.NumBlockBitmaps))
}

func (fs Fs) flushBalloc(op *awol.Op, bm balloc.Bitmap) {
	bm.Flush(op, fs.sb.blockAllocBase)
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

func (fs Fs) inodeRead(ino inode, boff uint64) disk.Block {
	return fs.log.Read(fs.sb.inodeBase + ino.btoa(boff))
}

func (fs Fs) inodeWrite(op *awol.Op, ino inode, boff uint64, b disk.Block) {
	op.Write(fs.sb.inodeBase+ino.btoa(boff), b)
}

func (fs Fs) checkInode(i Inum) {
	if i == 0 {
		panic("0 is an invalid inode number")
	}
	if i > fs.sb.numInodes {
		panic("invalid inode number")
	}
}

func (fs Fs) getInode(i Inum) inode {
	fs.checkInode(i)
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
	blockA := fs.readBalloc()
	for b := oldBlks; b < newBlks; b++ {
		newB := blockA.Alloc()
		if newB >= blockA.Size() {
			return false
		}
		ino.Direct[b] = newB
	}
	// TODO: it's brittle that we've modified the allocator only in the
	//  transaction; reading your own writes would make this easier to
	//  implement, but I'm not sure it makes the abstraction and invariants
	//  easier.
	fs.flushBalloc(op, blockA)
	ino.NBytes = newLen
	return true
}

func (fs Fs) shrinkInode(op *awol.Op, ino inode, newLen uint64) {
	if !(newLen <= ino.NBytes) {
		panic("shrinkInode requires a smaller length")
	}
	oldBlks := divUp(ino.NBytes, disk.BlockSize)
	newBlks := divUp(newLen, disk.BlockSize)
	blockA := fs.readBalloc()
	// newBlks <= oldBlks
	for b := newBlks; b <= oldBlks; b++ {
		oldB := ino.btoa(b)
		blockA.Free(oldB)
	}
	// TODO: same problem as in growInode of flushing the allocator
	fs.flushBalloc(op, blockA)
	ino.NBytes = newLen
}

func (fs Fs) lookupDir(dir inode, name string) Inum {
	if dir.Kind != INODE_KIND_DIR {
		panic("lookup on non-dir inode")
	}
	// invariant: directories always have length a multiple of BlockSize
	blocks := dir.NBytes / disk.BlockSize
	for b := uint64(0); b < blocks; b++ {
		de := decodeDirEnt(fs.inodeRead(dir, b))
		if !de.Valid {
			continue
		}
		if de.Name == name {
			return de.I
		}
	}
	return 0
}

func (fs Fs) findFreeDirEnt(op *awol.Op, dir inode) (uint64, bool) {
	// invariant: directories always have length a multiple of BlockSize
	blocks := dir.NBytes / disk.BlockSize
	for b := uint64(0); b < blocks; b++ {
		de := decodeDirEnt(fs.inodeRead(dir, b))
		if !de.Valid {
			return b, false
		}
	}
	// nothing free, allocate a new one
	ok := fs.growInode(op, dir, dir.NBytes+disk.BlockSize)
	if !ok {
		return 0, false
	}
	return blocks, false
}

// createDir creates a pointer name to i in the directory dir
//
// returns false if this fails (eg, due to allocation failure)
func (fs Fs) createDir(op *awol.Op, dir inode, name string, i Inum) bool {
	if dir.Kind != INODE_KIND_DIR {
		panic("create on non-dir inode")
	}
	fs.checkInode(i)
	b, ok := fs.findFreeDirEnt(op, dir)
	if !ok {
		return false
	}
	fs.inodeWrite(op, dir, b, encodeDirEnt(&DirEnt{
		Valid: true,
		Name:  name,
		I:     i,
	}))
	return true
}

// removeLink removes the link from name in dir
//
// returns true if a link was removed, false if name was not found
func (fs Fs) removeLink(op *awol.Op, dir inode, name string) bool {
	if dir.Kind != INODE_KIND_DIR {
		panic("remove on non-dir inode")
	}
	blocks := dir.NBytes / disk.BlockSize
	for b := uint64(0); b < blocks; b++ {
		de := decodeDirEnt(fs.inodeRead(dir, b))
		if !de.Valid {
			continue
		}
		if de.Name == name {
			fs.inodeWrite(op, dir, b, encodeDirEnt(&DirEnt{
				Valid: false,
				Name:  "",
				I:     0,
			}))
			return true
		}
	}
	return false
}
