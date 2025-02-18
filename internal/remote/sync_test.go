package remote

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMakeRelativeMap(t *testing.T) {
	tests := []struct {
		name      string
		basepath  string
		input     []string
		expected  map[string]bool
		expectErr bool
	}{
		{
			name:     "Normal case with relative paths",
			basepath: "/home/user",
			input:    []string{"/home/user/file1.txt", "/home/user/docs/file2.txt"},
			expected: map[string]bool{"file1.txt": true, "docs/file2.txt": true},
		},
		{
			name:     "Handles paths with slashes",
			basepath: "/home/user/",
			input:    []string{"/home/user//file1.txt", "/home/user/docs//file2.txt"},
			expected: map[string]bool{"file1.txt": true, "docs/file2.txt": true},
		},
		{
			name:     "Basepath is input itself",
			basepath: "/home/user/docs",
			input:    []string{"/home/user/docs"},
			expected: map[string]bool{".": true},
		},
		// TODO: fix
		//{
		//	name:      "Error case - Unrelated path",
		//	basepath:  "/home/user",
		//	input:     []string{"/other/path/file.txt"},
		//	expected:  nil,
		//	expectErr: true,
		//},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := makeRelativeMap(tt.basepath, tt.input)

			// Check for expected error
			if tt.expectErr {
				assert.Error(t, err, "Expected an error")
			} else {
				assert.NoError(t, err, "Did not expect an error")
				assert.Equal(t, tt.expected, result, "Expected output does not match")
			}
		})
	}
}
