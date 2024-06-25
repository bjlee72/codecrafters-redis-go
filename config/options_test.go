package config

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpts_Master_Validate(t *testing.T) {
	var o Opts
	err := o.Validate()
	require.NoError(t, err)

	assert.Equal(t, o.Role, "master")
	assert.NotNil(t, o.ReplicationID)
	assert.Len(t, o.ReplicationID, 40)
}

func TestOpts_ReplicaOf_Validate(t *testing.T) {
	type fields struct {
		ReplicaOf  string
		Role       string
		MasterIP   net.IP
		MasterPort int
	}
	tests := []struct {
		name    string
		fields  fields
		want    fields
		wantErr bool
	}{
		{
			name: "replicaof test",
			fields: fields{
				ReplicaOf: "127.0.0.2 8080",
			},
			want: fields{
				ReplicaOf:  "127.0.0.2 8080",
				Role:       "slave",
				MasterIP:   net.ParseIP("127.0.0.2"),
				MasterPort: 8080,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := &Opts{
				ReplicaOf:  tt.fields.ReplicaOf,
				Role:       tt.fields.Role,
				MasterIP:   tt.fields.MasterIP,
				MasterPort: tt.fields.MasterPort,
			}
			err := o.Validate()
			if err != nil && !tt.wantErr {
				t.Errorf("Opts.Validate() error = %v, wantErr %v", err, tt.wantErr)
			} else if err == nil {
				assert.Equal(t, o.ReplicaOf, tt.want.ReplicaOf)
				assert.Equal(t, o.MasterIP, tt.want.MasterIP)
				assert.Equal(t, o.MasterPort, tt.want.MasterPort)
				assert.Equal(t, o.Role, tt.want.Role)
			}
		})
	}
}
