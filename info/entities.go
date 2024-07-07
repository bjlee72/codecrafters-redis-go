package info

import (
	"fmt"
	"strings"
)

type Info struct {
	Replication Replication
}

type Replication struct {
	Role             string
	MasterReplID     string
	MasterReplOffset int
}

func (info Info) Info() []string {
	res := make([]string, 0)

	res = append(res, replicationInfo(&info.Replication))

	return res
}

func replicationInfo(repl *Replication) string {
	res := make([]string, 0)

	res = append(res, "# Replication")
	res = append(res, fmt.Sprintf("role:%v", repl.Role))
	if repl.Role == "master" {
		res = append(res, fmt.Sprintf("master_replid:%v", repl.MasterReplID))
		res = append(res, fmt.Sprintf("master_repl_offset:%v", repl.MasterReplOffset))
	}

	return strings.Join(res, "\r\n")
}
