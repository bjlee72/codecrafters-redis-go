package info

import (
	"fmt"
	"reflect"
	"strings"
)

func (info Info) ToRedisBulkString() (string, error) {
	typ := reflect.TypeOf(info)
	val := reflect.ValueOf(info)
	res := make([]string, 0)

	for i := 0; i < typ.NumField(); i++ {
		section := typ.Field(i)
		if key := section.Tag.Get("status_section"); key != "" {
			res = append(res, fmt.Sprintf("# %s", section.Name))

			var (
				sectionType  = section.Type
				sectionValue = val.Field(i)
			)

			fstr, err := toRedisBulkString(sectionType, sectionValue)
			if err != nil {
				return "", fmt.Errorf("cannot redis-marshall section %v", section.Name)
			}

			res = append(res, fstr)
		}
	}

	joined := strings.Join(res, "\r\n")
	return fmt.Sprintf("$%v\r\n%v\r\n", len(joined), joined), nil
}

func toRedisBulkString(sectionType reflect.Type, sectionValue reflect.Value) (string, error) {
	res := make([]string, 0)

	for i := 0; i < sectionType.NumField(); i++ {
		fld := sectionType.Field(i)
		if key := fld.Tag.Get("status_field"); key != "" {
			raw := sectionValue.Field(i)
			if raw.Kind() == reflect.String {
				res = append(res, fmt.Sprintf("%v:%v", key, raw.String()))
			}
		}
	}

	return strings.Join(res, "\r\n"), nil
}
