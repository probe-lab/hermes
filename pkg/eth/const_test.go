package eth

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_SupportedForkDigest(t *testing.T) {
	// Tests completeness
	for _, forkDigest := range SupportedForkDigests {
		t.Run(fmt.Sprintf("parse fork digest %s", forkDigest), func(t *testing.T) {
			_, err := ParseForkDigest(string(forkDigest))
			assert.NoError(t, err)
		})

		t.Run(fmt.Sprintf("genesis for fork digest %s", forkDigest), func(t *testing.T) {
			_, err := GenesisForForkDigest(forkDigest)
			assert.NoError(t, err)
		})
	}
}
