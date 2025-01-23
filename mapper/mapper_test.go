package mapper

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDecode(t *testing.T) {
	t.Parallel()

	t.Run("Success decoding", func(t *testing.T) {
		t.Parallel()

		input := map[string]interface{}{
			"key":          "value",
			"full_address": "Jl. Kebon Sirih No. 1",
		}

		var output struct {
			Key         string `sika:"key"`
			FullAddress string `sika:"full_address"`
		}

		err := Decode(input, &output)
		assert.NoError(t, err)
		assert.Equal(t, "value", output.Key)
		assert.Equal(t, "Jl. Kebon Sirih No. 1", output.FullAddress)
	})

	t.Run("Failed decoding", func(t *testing.T) {
		t.Parallel()

		input := map[string]interface{}{
			"key": make(chan int),
		}

		var output struct {
			Key         string `sika:"key"`
			FullAddress string `sika:"full_address"`
		}

		err := Decode(input, &output)
		assert.Error(t, err)
	})
}
