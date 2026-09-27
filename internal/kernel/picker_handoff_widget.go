package kernel

import (
	"context"
	_ "embed"
	"net/url"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const pickerHandoffURI = "ui://ouf/managed-file-upload-handoff-v3.html"

//go:embed picker_handoff.html
var pickerHandoffHTML string

func registerPickerUploadWidget(server *mcp.Server, pickerURL string) {
	u, _ := url.Parse(pickerURL) // Validated before registration.
	origin := u.Scheme + "://" + u.Host
	server.AddResource(&mcp.Resource{URI: pickerHandoffURI, Name: "OUF CSV upload", MIMEType: "text/html;profile=mcp-app"},
		func(_ context.Context, _ *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
			return &mcp.ReadResourceResult{Contents: []*mcp.ResourceContents{{
				URI: pickerHandoffURI, MIMEType: "text/html;profile=mcp-app", Text: pickerHandoffHTML,
				Meta: mcp.Meta{"openai/widgetCSP": map[string]any{"redirect_domains": []string{origin}},
					"ui":                       map[string]any{"prefersBorder": true},
					"openai/widgetDescription": "Scegli un CSV in OUF; l'Asset ID resta visibile nel riquadro e l'host può proporre un messaggio alla chat."},
			}}}, nil
		})
}
