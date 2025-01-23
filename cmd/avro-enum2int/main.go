package main

import (
	"context"
	"fmt"
	"io"
	"iter"
	"log"
	"os"
	"strings"

	ae "github.com/takanoriyanagitani/go-avro-enum2int"
	dh "github.com/takanoriyanagitani/go-avro-enum2int/avro/dec/hamba"
	eh "github.com/takanoriyanagitani/go-avro-enum2int/avro/enc/hamba"
	nh "github.com/takanoriyanagitani/go-avro-enum2int/avro/enum/hamba"
	. "github.com/takanoriyanagitani/go-avro-enum2int/util"
)

var EnvValByKey func(string) IO[string] = Lift(
	func(key string) (string, error) {
		val, found := os.LookupEnv(key)
		switch found {
		case true:
			return val, nil
		default:
			return "", fmt.Errorf("env var %s missing", key)
		}
	},
)

var stdin2maps IO[iter.Seq2[map[string]any, error]] = dh.
	StdinToMapsDefault

var schemaFilename IO[string] = EnvValByKey("ENV_SCHEMA_FILENAME")

func FilenameToStringLimited(limit int64) func(string) IO[string] {
	return Lift(func(filename string) (string, error) {
		f, e := os.Open(filename)
		if nil != e {
			return "", e
		}

		limited := &io.LimitedReader{
			R: f,
			N: limit,
		}

		var buf strings.Builder
		_, e = io.Copy(&buf, limited)
		return buf.String(), e
	})
}

const SchemaFileSizeMaxDefault int64 = 1048576

var enumSchemaFilename IO[string] = EnvValByKey("ENV_ENUM_SCHEMA_FILENAME")

var enumSchemaContent IO[string] = Bind(
	enumSchemaFilename,
	FilenameToStringLimited(SchemaFileSizeMaxDefault),
)

var schemaContent IO[string] = Bind(
	schemaFilename,
	FilenameToStringLimited(SchemaFileSizeMaxDefault),
)

var colname2cmap IO[ae.ColumnNameToConvMap] = Bind(
	enumSchemaContent,
	Lift(func(s string) (ae.ColumnNameToConvMap, error) {
		return nh.SchemaToColumnNameToMap(s), nil
	}),
)

var colname IO[ae.ColumnName] = Bind(
	EnvValByKey("ENV_ENUM_COL_NAME"),
	Lift(func(s string) (ae.ColumnName, error) {
		return ae.ColumnName(s), nil
	}),
)

var enum2imap IO[ae.EnumToIntMap] = Bind(
	colname2cmap,
	func(conv ae.ColumnNameToConvMap) IO[ae.EnumToIntMap] {
		return Bind(colname, Lift(conv))
	},
)

var enum2int IO[ae.EnumToInt] = Bind(
	enum2imap,
	Lift(func(m ae.EnumToIntMap) (ae.EnumToInt, error) {
		return m.ToEnumToIntDefault(), nil
	}),
)

var maps2maps IO[ae.MapsToMaps] = Bind(
	enum2int,
	func(c ae.EnumToInt) IO[ae.MapsToMaps] {
		return Bind(
			colname,
			Lift(func(n ae.ColumnName) (ae.MapsToMaps, error) {
				return c.ToMapsToMaps(n), nil
			}),
		)
	},
)

var mapd IO[iter.Seq2[map[string]any, error]] = Bind(
	maps2maps,
	func(c ae.MapsToMaps) IO[iter.Seq2[map[string]any, error]] {
		return Bind(
			stdin2maps,
			Lift(func(
				m iter.Seq2[map[string]any, error],
			) (iter.Seq2[map[string]any, error], error) {
				return c(m), nil
			}),
		)
	},
)

var stdin2avro2maps2mapd2avro2stdout IO[Void] = Bind(
	schemaContent,
	func(s string) IO[Void] {
		return Bind(
			mapd,
			eh.SchemaToMapsToStdoutDefault(s),
		)
	},
)

var sub IO[Void] = func(ctx context.Context) (Void, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	return stdin2avro2maps2mapd2avro2stdout(ctx)
}

func main() {
	_, e := sub(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}
