package marshal

import (
	"reflect"
	"testing"
)

func testRoundTrip(t *testing.T,
	expected interface{},
	encF func(enc Enc),
	decF func(dec Dec) interface{}) {
	t.Helper()
	enc := NewEnc()
	encF(enc)
	dec := NewDec(enc.Finish())
	actual := decF(dec)

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("got %v, expected %v", actual, expected)
	}
}

func TestPutInt(t *testing.T) {
	testRoundTrip(t, uint64(32), func(enc Enc) {
		enc.PutInt(32)
	}, func(dec Dec) interface{} {
		return dec.GetInt()
	})
	testRoundTrip(t, uint64(1<<45+1<<50), func(enc Enc) {
		enc.PutInt(1<<45 + 1<<50)
	}, func(dec Dec) interface{} {
		return dec.GetInt()
	})
}

func TestPutInts(t *testing.T) {
	for _, x := range [][]uint64{
		{34, 100, 1<<20 + 1<<40 + 1<<50, 153 << 34},
		{},
		{2},
	} {
		testRoundTrip(t, x, func(enc Enc) {
			enc.PutInts(x)
		}, func(dec Dec) interface{} {
			return dec.GetInts(len(x))
		})
	}
}

func TestPutBool(t *testing.T) {
	for _, x := range []bool{true, false} {
		testRoundTrip(t, x, func(enc Enc) {
			enc.PutBool(x)
		}, func(dec Dec) interface{} {
			return dec.GetBool()
		})
	}
}

type Various struct {
	a uint64
	b string
	c bool
	d string
	e []byte // fixed length 2
}

func TestVarious(t *testing.T) {
	for _, x := range []Various{
		{34,
			"foo",
			false,
			"",
			[]byte{3, 4}},
		{0,
			string([]byte{23, 0, 0, 0xff}),
			true,
			"hello there\n\000",
			[]byte{0, 7}},
	} {
		testRoundTrip(t, x,
			func(enc Enc) {
				enc.PutInt(x.a)
				enc.PutString(x.b)
				enc.PutBool(x.c)
				enc.PutString(x.d)
				enc.PutBytes(x.e)
			},
			func(dec Dec) interface{} {
				var x Various
				x.a = dec.GetInt()
				x.b = dec.GetString()
				x.c = dec.GetBool()
				x.d = dec.GetString()
				x.e = dec.GetBytes(2)
				return x
			})
	}
}
