package protocol

import (
	"fmt"
	"strings"
)

func ToBulkStringArray(request []string) string {
	// recover the request to the original format
	blkStrings := make([]string, 0)
	for _, seg := range request {
		blkStrings = append(blkStrings, fmt.Sprintf("$%d\r\n%s", len(seg), seg))
	}

	return fmt.Sprintf("*%d\r\n%s\r\n", len(blkStrings), strings.Join(blkStrings, "\r\n"))
}
