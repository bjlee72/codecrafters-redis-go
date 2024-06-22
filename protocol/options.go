package protocol

import (
	"fmt"
	"strings"
)

type OptionConfig map[string]int

func BuildOptions(requestArray []string, cfg OptionConfig) (map[string][]string, error) {
	ret := make(map[string][]string)
	idx := 0
	for idx < len(requestArray) {
		key := strings.ToUpper(requestArray[idx])
		value := make([]string, 0)

		l, ok := cfg[key]
		if !ok {
			return nil, fmt.Errorf("option key %v is not supported by the command", key)
		}

		if idx+1+l <= len(requestArray) {
			// the remaining elements in the requestArray is less than the configured option value length.
			value = requestArray[idx+1 : idx+1+l]
		}
		ret[key] = value
		idx = idx + 1 + l
	}
	return ret, nil
}
