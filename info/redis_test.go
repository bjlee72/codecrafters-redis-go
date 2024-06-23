package info

import "testing"

func TestInfo_ToRedisBulkString(t *testing.T) {
	type fields struct {
		Info Info
	}
	tests := []struct {
		name    string
		fields  fields
		want    string
		wantErr bool
	}{
		{
			name: "basic test",
			fields: fields{
				Info: Info{
					Replication: Replication{
						Role: "master",
					},
				},
			},
			want: "$26\r\n# Replication\r\nrole:master\r\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.fields.Info.ToRedisBulkString()
			if (err != nil) != tt.wantErr {
				t.Errorf("Info.ToRedisBulkString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Info.ToRedisBulkString() = %v, want %v", got, tt.want)
			}
		})
	}
}
