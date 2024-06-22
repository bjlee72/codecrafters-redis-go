package protocol

import (
	"fmt"
	"strconv"
)

// ValidateArray validates the given array type '*#' and returns the number after '*'
func ValidateArray(str string) (int, error) {
	if str[0] != '*' {
		return 0, fmt.Errorf("wrong array token '%s'", str)
	}

	ret, err := strconv.Atoi(str[1:])
	if err != nil {
		return 0, fmt.Errorf("wrong int format after '*': '%s'", str)
	}

	return ret, nil
}

func ValidateBulkString(str string) (int, error) {
	if str[0] != '$' {
		return 0, fmt.Errorf("not a bulk string token '%s'", str)
	}

	ret, err := strconv.Atoi(str[1:])
	if err != nil {
		return 0, fmt.Errorf("wrong int format after '$': '%s'", str)
	}

	return ret, nil
}
