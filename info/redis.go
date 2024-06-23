package info

import (
	"fmt"
	"reflect"
	"strings"
)

func (info Info) ToRedisBulkString() (string, error) {
	val := reflect.ValueOf(info)
	res := make([]string, 0)

	for i := 0; i < val.Type().NumField(); i++ {
		section := val.Type().Field(i)
		if key := section.Tag.Get("status_section"); key != "" {
			res = append(res, fmt.Sprintf("# %s", section.Name))

			fstr, err := toRedisBulkString(val.Field(i))
			if err != nil {
				return "", fmt.Errorf("toRedisBulkString failed: %v", err)
			}

			res = append(res, fstr)
		}
	}

	joined := strings.Join(res, "\r\n")
	return fmt.Sprintf("$%v\r\n%v\r\n", len(joined), joined), nil
}

func toRedisBulkString(section reflect.Value) (string, error) {
	res := make([]string, 0)

	for i := 0; i < section.Type().NumField(); i++ {
		fld := section.Type().Field(i)
		if key := fld.Tag.Get("status_field"); key != "" {
			raw := section.Field(i)
			if raw.Kind() == reflect.String {
				res = append(res, fmt.Sprintf("%v:%v", key, raw.String()))
			}
		}
	}

	return strings.Join(res, "\r\n"), nil
}
