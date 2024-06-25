package info

import (
	"fmt"
	"strings"
)

func (info Info) ToRedisBulkString() (string, error) {
	res := make([]string, 0)

	res = append(res, replicationToRedisBulkString(&info.Replication))

	joined := strings.Join(res, "\r\n")
	return fmt.Sprintf("$%v\r\n%v\r\n", len(joined), joined), nil
}

func replicationToRedisBulkString(repl *Replication) string {
	res := make([]string, 0)

	res = append(res, "# Replication")
	res = append(res, fmt.Sprintf("role:%v", repl.Role))
	if repl.Role == "master" {
		res = append(res, fmt.Sprintf("master_replid:%v", repl.MasterReplID))
		res = append(res, fmt.Sprintf("master_repl_offset:%v", repl.MasterReplOffset))
	}

	return strings.Join(res, "\r\n")
}
