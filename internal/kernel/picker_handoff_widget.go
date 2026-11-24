package kernel

import (
	"context"
	_ "embed"
	"encoding/json"
	"net/url"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const pickerHandoffURI = "ui://ouf/managed-file-upload-handoff-v3.html"

//go:embed picker_handoff.html
var pickerHandoffHTML string

func registerPickerUploadWidget(server *mcp.Server, pickerURL string) {
	u, _ := url.Parse(pickerURL) // Validated before registration.
	origin := u.Scheme + "://" + u.Host
	encoded, _ := json.Marshal(origin)
	html := strings.Replace(pickerHandoffHTML, `"__OUF_PICKER_ORIGIN__"`, string(encoded), 1)
	server.AddResource(&mcp.Resource{URI: pickerHandoffURI, Name: "OUF file upload", MIMEType: "text/html;profile=mcp-app"},
		func(_ context.Context, _ *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
			return &mcp.ReadResourceResult{Contents: []*mcp.ResourceContents{{
				URI: pickerHandoffURI, MIMEType: "text/html;profile=mcp-app", Text: html,
				Meta: mcp.Meta{"openai/widgetCSP": map[string]any{"redirect_domains": []string{origin}},
					"ui":                       map[string]any{"prefersBorder": true},
					"openai/widgetDescription": "Apri il selettore OUF. La chat attende la scelta del file."},
			}}}, nil
		})
}
