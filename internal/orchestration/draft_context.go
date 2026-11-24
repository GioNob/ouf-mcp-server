package orchestration

import "encoding/json"

// Context for the MCP decision is not part of the owner command, its
// fingerprint or its idempotency hash. Keep predecessor bytes verbatim.
func withoutDraftContext(raw json.RawMessage) (json.RawMessage, error) {
	var args map[string]json.RawMessage
	if err := json.Unmarshal(raw, &args); err != nil {
		return nil, err
	}
	if args["draftAssetRef"] == nil && args["revisionAssetRef"] == nil {
		return raw, nil
	}
	delete(args, "draftAssetRef")
	delete(args, "revisionAssetRef")
	return json.Marshal(args)
}
