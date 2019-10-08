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

func Open(bm []disk.Block) Bitmap {
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
	byteIndex := (off / 8) % 4096
	bitIndex := off % 8
	bm[blockIndex][byteIndex] = bm[blockIndex][byteIndex] & ^(1 << bitIndex)
}

func (bm Bitmap) Size() uint64 {
	return ItemsPerBitmap * uint64(len(bm))
}

// Allocate an item in a bitmap
//
// modifies bm to mark the item allocated
//
// boolean status is false if allocator is full
func (bm Bitmap) Alloc() (uint64, bool) {
	for blockIndex := uint64(0); blockIndex < uint64(len(bm)); blockIndex++ {
		b := bm[blockIndex]
		for byteIndex := uint64(0); byteIndex < uint64(len(b)); byteIndex++ {
			byteVal := b[byteIndex]
			if byteVal == 0xff {
				continue
			}
			for bitIndex := uint64(0); bitIndex < 8; bitIndex++ {
				if byteVal&(1<<bitIndex) == 0 {
					b[byteIndex] |= 1 << bitIndex
					off := bitIndex + 8*byteIndex + ItemsPerBitmap*blockIndex
					return off, true
				}
			}
		}
	}
	return 0, false
}
