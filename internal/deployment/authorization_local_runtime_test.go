package deployment

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAuthorizationLocalRuntimeContract(t *testing.T) {
	mainRaw, err := os.ReadFile(filepath.Join("..", "..", "cmd", "ouf-mcp", "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	mainText := string(mainRaw)
	for _, required := range []string{
		"MCP_AUTHORIZATION_BUNDLE_ENDPOINT",
		"MCP_AUTHORIZATION_BUNDLE_REFRESH",
		"authorization.NewCache",
		"authCache.Refresh",
		"retaining last known good bundle",
		"Auth: authCache",
	} {
		if !strings.Contains(mainText, required) {
			t.Fatalf("local Authorization runtime wiring missing %q", required)
		}
	}
	for _, forbidden := range []string{"MCP_AUTHORIZATION_ENDPOINT", "NewAuthorization("} {
		if strings.Contains(mainText, forbidden) {
			t.Fatalf("synchronous request-path Authorization stub remains wired: %q", forbidden)
		}
	}

	kernelRaw, err := os.ReadFile(filepath.Join("..", "kernel", "kernel.go"))
	if err != nil {
		t.Fatal(err)
	}
	kernelText := string(kernelRaw)
	for _, required := range []string{
		"X-OUF-Token-Issuer",
		"X-OUF-Token-Audience",
		"X-OUF-Granted-Scopes",
	} {
		if !strings.Contains(kernelText, required) {
			t.Fatalf("trusted principal context propagation missing %q", required)
		}
	}
}
