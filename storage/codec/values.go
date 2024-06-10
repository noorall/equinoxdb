package codec

import (
	"equinox/storage/types"
	"fmt"
)

func EncodeValues(a types.Values, buf []byte) ([]byte, error) {
	if len(a) == 0 {
		panic("unable to encode block type")
	}

	switch a[0].(type) {
	case types.FloatValue:
		return EncodeFloatBlock(buf, a)
	case types.IntegerValue:
		return EncodeIntegerBlock(buf, a)
	case types.UnsignedValue:
		return EncodeUnsignedBlock(buf, a)
	case types.BooleanValue:
		return EncodeBooleanBlock(buf, a)
	case types.StringValue:
		return EncodeStringBlock(buf, a)
	}

	return nil, fmt.Errorf("unsupported value type %T", a[0])
}
