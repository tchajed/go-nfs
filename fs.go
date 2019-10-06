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
		newB, ok := blockA.Alloc()
		if !ok {
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

// createLink creates a pointer name to i in the directory dir
//
// returns false if this fails (eg, due to allocation failure)
func (fs Fs) createLink(op *awol.Op, dir inode, name string, i Inum) bool {
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

func (fs Fs) isDirEmpty(dir inode) bool {
	if dir.Kind != INODE_KIND_DIR {
		panic("remove on non-dir inode")
	}
	blocks := dir.NBytes / disk.BlockSize
	for b := uint64(0); b < blocks; b++ {
		de := decodeDirEnt(fs.inodeRead(dir, b))
		if de.Valid {
			return false
		}
	}
	return true
}

// readDirEntries reads all of the entries in dir
//
// NFS's readdir operation has a more sophisticated cookie/cookie verifier
// mechanism for paging and reporting iterator invalidation.
func (fs Fs) readDirEntries(dir inode) []string {
	names := make([]string, 0)
	blocks := dir.NBytes / disk.BlockSize
	for b := uint64(0); b < blocks; b++ {
		de := decodeDirEnt(fs.inodeRead(dir, b))
		if !de.Valid {
			continue
		}
		names = append(names, de.Name)
	}
	return names
}

// file-system API

func (fs Fs) RootInode() Inum {
	return fs.sb.rootInode
}

func (fs Fs) Lookup(i Inum, name string) Inum {
	dir := fs.getInode(i)
	return fs.lookupDir(dir, name)
}

func (fs Fs) Create(dirI Inum, name string, unchecked bool) (Inum, bool) {
	op := fs.log.Begin()
	dir := fs.getInode(dirI)
	if unchecked {
		fs.removeLink(op, dir, name)
	}
	i, ino := fs.findFreeInode()
	if i == 0 {
		return 0, false
	}
	ok := fs.createLink(op, dir, name, i)
	if !ok {
		return 0, false
	}
	ino.Kind = INODE_KIND_FILE
	fs.flushInode(op, i, ino)
	fs.log.Commit(op)
	return i, true
}

func (fs Fs) Mkdir(dirI Inum, name string) (Inum, bool) {
	op := fs.log.Begin()
	dir := fs.getInode(dirI)
	i, ino := fs.findFreeInode()
	if i == 0 {
		return 0, false
	}
	ino.Kind = INODE_KIND_DIR
	ok := fs.createLink(op, dir, name, i)
	if !ok {
		return 0, false
	}
	fs.flushInode(op, i, ino)
	fs.log.Commit(op)
	return i, true
}

func (fs Fs) Read(i Inum, off uint64, length uint64) ([]byte, bool) {
	ino := fs.getInode(i)
	if ino.Kind != INODE_KIND_FILE {
		return nil, false
	}
	if off+length > ino.NBytes {
		return nil, false
	}
	bs := make([]byte, 0, length)
	for boff := off / disk.BlockSize; length > 0; boff++ {
		b := fs.inodeRead(ino, boff)
		if off%disk.BlockSize != 0 {
			byteOff := off % disk.BlockSize
			b = b[byteOff:]
		}
		if length < uint64(len(b)) {
			b = b[:length]
		}
		bs = append(bs, b...)
		length -= uint64(len(b))
	}
	return bs, true
}

func (fs Fs) Write(i Inum, off uint64, bs []byte) bool {
	op := fs.log.Begin()
	ino := fs.getInode(i)
	if ino.Kind != INODE_KIND_FILE {
		return false
	}
	for boff := off / disk.BlockSize; len(bs) > 0; boff++ {
		if off%disk.BlockSize != 0 {
			b := fs.inodeRead(ino, boff)
			byteOff := off % disk.BlockSize
			nBytes := disk.BlockSize - byteOff
			if uint64(len(bs)) < nBytes {
				nBytes = uint64(len(bs))
			}
			for i := byteOff; i < nBytes; i++ {
				b[byteOff+i] = bs[i]
			}
			fs.inodeWrite(op, ino, boff, b)
			bs = bs[nBytes:]
			off += nBytes
		} else if uint64(len(bs)) < disk.BlockSize {
			b := fs.inodeRead(ino, boff)
			for i := 0; i < len(bs); i++ {
				b[i] = bs[i]
			}
			fs.inodeWrite(op, ino, boff, b)
			bs = nil
		} else {
			fs.inodeWrite(op, ino, boff, disk.Block(bs[:disk.BlockSize]))
			bs = bs[disk.BlockSize:]
			off += disk.BlockSize
		}
	}
	fs.log.Commit(op)
	return true
}

func (fs Fs) Readdir(i Inum) []string {
	dir := fs.getInode(i)
	return fs.readDirEntries(dir)
}

func (fs Fs) Remove(dirI Inum, name string) bool {
	op := fs.log.Begin()
	dir := fs.getInode(dirI)
	i := fs.lookupDir(dir, name)
	if i == 0 {
		return false
	}
	ino := fs.getInode(i)
	if ino.Kind == INODE_KIND_FREE {
		panic("directory entries should point to valid inodes")
	}
	if ino.Kind == INODE_KIND_DIR {
		if !fs.isDirEmpty(ino) {
			// cannot unlink non-empty directory
			return false
		}
	}
	ok := fs.removeLink(op, dir, name)
	if !ok {
		return false
	}
	fs.log.Commit(op)
	return true
}
