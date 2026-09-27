package kernel

import (
	"strings"
	"testing"
)

func TestPickerHandoffUsesConfiguredInstallationURL(t *testing.T) {
	const configured = "https://gateway.altro-ente.example/trusted-human/managed-files/"
	html := renderPickerHandoffHTML(configured)
	if strings.Contains(html, "__OUF_PICKER_PREFIX__") {
	    t.Fatal("picker placeholder was not rendered")
	}
	if !strings.Contains(html, configured+"?handoff=") {
	    t.Fatal("picker does not use configured installation URL")
	}
	if strings.Contains(html, "api.ouf-lab.it") {
	    t.Fatal("picker still embeds lab hostname")
	}
}
