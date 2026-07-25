package anhe

import "testing"

func TestBuildReadQuerySelectiveExpansion(t *testing.T) {
	tests := []struct {
		name string
		opts GetOptions
		want string
	}{
		{
			name: "default",
			want: "GET vape:1;",
		},
		{
			name: "raw",
			opts: GetOptions{ExpandMode: ExpandNone},
			want: "GET vape:1 RAW;",
		},
		{
			name: "only",
			opts: GetOptions{
				ExpandMode:  ExpandOnly,
				ExpandPaths: []string{"/checkin", "/owner", "/rewards"},
			},
			want: "GET vape:1 EXPAND ONLY /checkin,/owner,/rewards;",
		},
		{
			name: "except",
			opts: GetOptions{
				ExpandMode:  ExpandExcept,
				ExpandPaths: []string{"/chests"},
			},
			want: "GET vape:1 EXPAND EXCEPT /chests;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := buildReadQuery("GET vape:1", tt.opts)
			if err != nil {
				t.Fatalf("build query: %v", err)
			}
			if got != tt.want {
				t.Fatalf("unexpected query: got %q want %q", got, tt.want)
			}
		})
	}
}

func TestBuildReadQueryRejectsInvalidPaths(t *testing.T) {
	_, err := buildReadQuery("GET vape:1", GetOptions{
		ExpandMode:  ExpandOnly,
		ExpandPaths: []string{"checkin"},
	})
	if err == nil {
		t.Fatal("expected invalid path error")
	}
}
