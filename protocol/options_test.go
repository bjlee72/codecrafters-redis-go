package protocol

import (
	"reflect"
	"testing"
)

func Test_buildOptions(t *testing.T) {
	type args struct {
		requestArray []string
		cfg          OptionConfig
	}
	tests := []struct {
		name    string
		args    args
		want    map[string][]string
		wantErr bool
	}{
		{
			name: "happy case #1",
			args: args{
				requestArray: []string{"MX", "0", "FX", "0", "1", "DX"},
				cfg:          OptionConfig{"MX": 1, "FX": 2, "DX": 0},
			},
			want: map[string][]string{
				"MX": {"0"},
				"FX": {"0", "1"},
				"DX": {},
			},
			wantErr: false,
		},
		{
			name: "happy case #2",
			args: args{
				requestArray: []string{"Mx", "0", "FX", "0", "1", "DX"},
				cfg:          OptionConfig{"MX": 1, "FX": 2, "DX": 0},
			},
			want: map[string][]string{
				"MX": {"0"},
				"FX": {"0", "1"},
				"DX": {},
			},
			wantErr: false,
		},
		{
			name: "not happy case #1 - option length mismatch (FX)",
			args: args{
				requestArray: []string{"MX", "0", "FX", "0", "1", "DX"},
				cfg:          OptionConfig{"MX": 1, "FX": 1, "DX": 0},
			},
			wantErr: true,
		},
		{
			name: "not happy case #2 - non-option key is included in the string",
			args: args{
				requestArray: []string{"MF", "0", "FX", "0", "1", "DX"},
				cfg:          OptionConfig{"MX": 1, "FX": 1, "DX": 0},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := BuildOptions(tt.args.requestArray, tt.args.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("buildOptions() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("buildOptions() = %v, want %v", got, tt.want)
			}
		})
	}
}
