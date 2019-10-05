package nfs

import (
	"github.com/tchajed/goose/machine/disk"

	"github.com/tchajed/go-nfs/marshal"
)

const MaxNameLen = 4096 - 1 - 8

type DirEnt struct {
	Valid bool
	Name  string // max 4096-1-8=4087 bytes
	I     Inum
}

func encodeDirEnt(de *DirEnt) disk.Block {
	if len(de.Name) > MaxNameLen {
		panic("directory entry name too long")
	}
	enc := marshal.NewEnc()
	enc.PutString(de.Name)
	enc.PutBool(de.Valid)
	enc.PutInt(de.I)
	return enc.Finish()
}

func decodeDirEnt(b disk.Block) *DirEnt {
	dec := marshal.NewDec(b)
	de := &DirEnt{}
	de.Name = dec.GetString()
	de.Valid = dec.GetBool()
	de.I = dec.GetInt()
	return de
}
