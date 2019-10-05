package marshal

import (
	"github.com/tchajed/goose/machine"
	"github.com/tchajed/goose/machine/disk"
)

type Enc struct {
	b   disk.Block
	off *uint64
}

func NewEnc() Enc {
	return Enc{b: make(disk.Block, disk.BlockSize), off: new(uint64)}
}

func (enc Enc) PutInt(x uint64) {
	off := *enc.off
	machine.UInt64Put(enc.b[off:off+8], x)
	*enc.off += 8
}

func (enc Enc) PutInts(xs []uint64) {
	// we could be slightly more efficient here by not repeatedly updating
	// the offset
	for _, x := range xs {
		enc.PutInt(x)
	}
}

func (enc Enc) PutBool(b bool) {
	off := *enc.off
	if b {
		enc.b[off] = 1
	} else {
		enc.b[off] = 0
	}
	*enc.off++
}

func (enc Enc) PutBytes(b []byte) {
	off := *enc.off
	for i := uint64(0); i < uint64(len(b)); i++ {
		enc.b[off+i] = b[i]
	}
	*enc.off += uint64(len(b))
}

func (enc Enc) PutString(s string) {
	enc.PutInt(uint64(len(s)))
	enc.PutBytes([]byte(s))
}

func (enc Enc) Finish() disk.Block {
	return enc.b
}

type Dec struct {
	b   disk.Block
	off *uint64
}

func NewDec(b disk.Block) Dec {
	return Dec{b: b, off: new(uint64)}
}

func (dec Dec) GetInt() uint64 {
	off := *dec.off
	x := machine.UInt64Get(dec.b[off : off+8])
	*dec.off += 8
	return x
}

func (dec Dec) GetInts(len int) []uint64 {
	xs := make([]uint64, len)
	for i := 0; i < len; i++ {
		xs[i] = dec.GetInt()
	}
	return xs
}

func (dec Dec) GetBool() bool {
	off := *dec.off
	x := dec.b[off]
	b := true
	if x == 0 {
		b = false
	}
	*dec.off++
	return b
}

func (dec Dec) GetBytes(length uint64) []byte {
	off := *dec.off
	bs := dec.b[off : off+length]
	*dec.off += length
	return bs
}

func (dec Dec) GetString() string {
	length := dec.GetInt()
	return string(dec.GetBytes(length))
}
