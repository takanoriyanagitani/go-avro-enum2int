package enumh

import (
	"errors"
	"iter"
	"maps"

	ha "github.com/hamba/avro/v2"
	ae "github.com/takanoriyanagitani/go-avro-enum2int"
)

var (
	ErrInvalidField  error = errors.New("invalid field")
	ErrInvalidSchema error = errors.New("invalid schema")
	ErrFieldMissing  error = errors.New("field missing")
)

func EnumSchemaToMap(es *ha.EnumSchema) map[string]int32 {
	var symbols []string = es.Symbols()

	var i iter.Seq2[string, int32] = func(
		yield func(string, int32) bool,
	) {
		for ix, s := range symbols {
			var index int32 = int32(ix)
			yield(s, index)
		}
	}

	return maps.Collect(i)
}

func FieldToMap(f *ha.Field) (map[string]int32, error) {
	var typ ha.Schema = f.Type()
	switch s := typ.(type) {
	case *ha.EnumSchema:
		return EnumSchemaToMap(s), nil
	default:
		return nil, ErrInvalidField
	}
}

func FieldsToMap(
	fields []*ha.Field,
	colname string,
) (map[string]int32, error) {
	for _, field := range fields {
		var name string = field.Name()
		if name == colname {
			return FieldToMap(field)
		}
	}
	return nil, ErrFieldMissing
}

func RecordSchemaToMap(
	r *ha.RecordSchema,
	colname string,
) (map[string]int32, error) {
	return FieldsToMap(r.Fields(), colname)
}

func SchemaToMapHamba(
	s ha.Schema,
	colname string,
) (map[string]int32, error) {
	switch typ := s.(type) {
	case *ha.RecordSchema:
		return RecordSchemaToMap(typ, colname)
	default:
		return nil, ErrInvalidSchema
	}
}

func SchemaToMap(
	schema string,
	colname string,
) (map[string]int32, error) {
	parsed, e := ha.Parse(schema)
	if nil != e {
		return nil, e
	}
	return SchemaToMapHamba(parsed, colname)
}

func SchemaToColumnNameToMap(schema string) ae.ColumnNameToConvMap {
	return func(cn ae.ColumnName) (ae.EnumToIntMap, error) {
		return SchemaToMap(schema, string(cn))
	}
}
