package balloc

import (
	"github.com/tchajed/go-awol"
	"github.com/tchajed/goose/machine/disk"
)

const ItemsPerBitmap = 4096 * 8

type Bitmap []disk.Block

func Init(blocks int) Bitmap {
	bm := make(Bitmap, blocks)
	for i := 0; i < blocks; i++ {
		bm[i] = make(disk.Block, disk.BlockSize)
	}
	return bm
}

func (bm Bitmap) Flush(op *awol.Op, at uint64) {
	for i, b := range bm {
		op.Write(at+uint64(i), b)
	}
}

func Open(log *awol.Log, at uint64, blocks int) Bitmap {
	bm := make(Bitmap, blocks)
	for i := 0; i < blocks; i++ {
		bm[i] = log.Read(at + uint64(i))
	}
	return bm
}

// Free an item in a bitmap
//
// modifies bm
//
// assumes off < bitmaps*ItemsPerBitmap
// (off fits in the abstract size of the bitmap)
func (bm Bitmap) Free(off uint64) {
	blockIndex := off / ItemsPerBitmap
	byteIndex := (off % ItemsPerBitmap) / 4096
	bitIndex := ((off % ItemsPerBitmap) % 4096) / 8
	bm[blockIndex][byteIndex] = bm[blockIndex][byteIndex] & ^(1 << bitIndex)
}

func (bm Bitmap) Size() uint64 {
	return ItemsPerBitmap * uint64(len(bm))
}

// Allocate an item in a bitmap
//
// modifies bm to mark the item allocated
//
// returns an out-of-bounds off (bm.Size()) if the bitmap is full
func (bm Bitmap) Alloc() uint64 {
	for off := uint64(0); off < bm.Size(); off++ {
		blockIndex := off / ItemsPerBitmap
		b := bm[blockIndex]
		byteIndex := (off % ItemsPerBitmap) / 4096
		bitIndex := ((off % ItemsPerBitmap) % 4096) / 8
		if b[byteIndex]&(1<<bitIndex) != 0 {
			b[byteIndex] |= 1 << bitIndex
			return off
		}
	}
	return bm.Size()
}
