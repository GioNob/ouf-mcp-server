package kernel

import (
	"context"
	_ "embed"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const attachmentProbeURI = "ui://ouf/attachment-read-probe.html"

//go:embed attachment_read_probe.html
var attachmentProbeHTML string

// The widget is a read-only feasibility check. It reads the ChatGPT file in
// the host UI and reports only an outcome and byte count locally in the UI.
// It cannot create an OUF asset or transfer bytes into an MCP tool argument.
func registerAttachmentProbeWidget(server *mcp.Server) {
	server.AddResource(&mcp.Resource{
		URI: attachmentProbeURI, Name: "OUF attachment read diagnostic",
		MIMEType: "text/html;profile=mcp-app",
	}, func(_ context.Context, _ *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		return &mcp.ReadResourceResult{Contents: []*mcp.ResourceContents{{
			URI: attachmentProbeURI, MIMEType: "text/html;profile=mcp-app",
			Text: attachmentProbeHTML,
			Meta: mcp.Meta{"ui": map[string]any{"csp": map[string]any{
				"connectDomains": []string{"https://*.oaiusercontent.com"},
			}}},
		}}}, nil
	})
}
