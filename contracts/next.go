package contracts

import (
	_ "embed"
	"encoding/json"
	"sync"

	"github.com/google/jsonschema-go/jsonschema"
)

//go:embed ouf-next-step-v1.json
var nextStepJSON []byte

// NextStepSchema resolves the published wire contract once at startup.
var NextStepSchema = sync.OnceValues(func() (*jsonschema.Resolved, error) {
	var schema jsonschema.Schema
	if err := json.Unmarshal(nextStepJSON, &schema); err != nil {
		return nil, err
	}
	return schema.Resolve(nil)
})
