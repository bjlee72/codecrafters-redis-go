package protocol

import "testing"

func TestMessage_ToR2_Array(t *testing.T) {
	type fields struct {
		tokens []string
		typ    MessageType
		redis  string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "array of bulk strings",
			fields: fields{
				tokens: []string{"SET", "fooo", "12"},
				typ:    Array,
			},
			want: "*3\r\n$3\r\nSET\r\n$4\r\nfooo\r\n$2\r\n12\r\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Message{
				tokens: tt.fields.tokens,
				typ:    tt.fields.typ,
				redis:  tt.fields.redis,
			}
			if got := m.ToR2(); got != tt.want {
				t.Errorf("Message.ToR2() = %v, want %v", got, tt.want)
			}
		})
	}
}
