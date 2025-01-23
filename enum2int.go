package enum2int

import (
	"database/sql"
	"errors"
	"fmt"
	"iter"
)

var (
	ErrInvalidInput error = errors.New("invalid input")
	ErrUnknownEnum  error = errors.New("unknown enum value")
)

type EnumToInt func(string) (int32, error)

func (c EnumToInt) ToNullable(s sql.Null[string]) (sql.Null[int32], error) {
	if !s.Valid {
		return sql.Null[int32]{
			Valid: false,
			V:     0,
		}, nil
	}

	mapd, e := c(s.V)

	return sql.Null[int32]{
		Valid: nil == e,
		V:     mapd,
	}, e
}

func (c EnumToInt) AnyToString(a any) (sql.Null[string], error) {
	var ret sql.Null[string]

	switch typ := a.(type) {
	case nil:
		return ret, nil
	case string:
		return sql.Null[string]{
			Valid: true,
			V:     typ,
		}, nil
	default:
		return ret, ErrInvalidInput
	}
}

func (c EnumToInt) AnyToNullable(a any) (sql.Null[int32], error) {
	var ret sql.Null[int32]

	n, e := c.AnyToString(a)
	if nil != e {
		return ret, e
	}

	return c.ToNullable(n)
}

type GenericNullable[T any] sql.Null[T]

func (g GenericNullable[T]) ToAny() any {
	switch g.Valid {
	case true:
		return g.V
	default:
		return nil
	}
}

func (c EnumToInt) AnyToNullableToAny(a any) (any, error) {
	n, e := c.AnyToNullable(a)
	if nil != e {
		return nil, e
	}

	return GenericNullable[int32](n).ToAny(), nil
}

type MapsToMaps func(
	iter.Seq2[map[string]any, error],
) iter.Seq2[map[string]any, error]

func (c EnumToInt) ToMapsToMaps(
	colname ColumnName,
) MapsToMaps {
	return func(
		original iter.Seq2[map[string]any, error],
	) iter.Seq2[map[string]any, error] {
		return func(yield func(map[string]any, error) bool) {
			buf := map[string]any{}

			for row, e := range original {
				clear(buf)

				if nil != e {
					yield(nil, e)
					return
				}

				for key, val := range row {
					buf[key] = val
				}

				var a any = row[string(colname)]
				mapd, e := c.AnyToNullableToAny(a)
				buf[string(colname)] = mapd

				if !yield(buf, e) {
					return
				}
			}
		}
	}
}

type EnumToIntMap map[string]int32

func MissingEnumValToError(val string) error {
	return fmt.Errorf("%w: %s", ErrUnknownEnum, val)
}

func (m EnumToIntMap) ToEnumToInt(onMissing func(string) error) EnumToInt {
	return func(enumVal string) (int32, error) {
		i, found := m[enumVal]
		switch found {
		case true:
			return i, nil
		default:
			return 0, onMissing(enumVal)
		}
	}
}

func (m EnumToIntMap) ToEnumToIntDefault() EnumToInt {
	return m.ToEnumToInt(MissingEnumValToError)
}

type ColumnName string

type ColumnNameToConvMap func(ColumnName) (EnumToIntMap, error)

const BlobSizeMaxDefault int = 1048576

type DecodeConfig struct{ BlobSizeMax int }

var DecodeConfigDefault DecodeConfig = DecodeConfig{
	BlobSizeMax: BlobSizeMaxDefault,
}

type Codec string

const (
	CodecNull    Codec = "null"
	CodecDeflate Codec = "deflate"
	CodecSnappy  Codec = "snappy"
	CodecZstd    Codec = "zstandard"
	CodecBzip2   Codec = "bzip2"
	CodecXz      Codec = "xz"
)

const BlockLengthDefault int = 100

type EncodeConfig struct {
	BlockLength int
	Codec
}

var EncodeConfigDefault EncodeConfig = EncodeConfig{
	BlockLength: BlockLengthDefault,
	Codec:       CodecNull,
}
