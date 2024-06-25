package info

type Info struct {
	Replication Replication
}

type Replication struct {
	Role             string
	MasterReplID     string
	MasterReplOffset int
}
