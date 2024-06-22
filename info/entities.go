package info

type Info struct {
	Replication Replication `status_section:"replication"`
}

type Replication struct {
	Role string `status_field:"role"`
}
