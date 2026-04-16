package worker

import (
	"bytes"
	"fmt"
	htmltemplate "html/template"
	texttemplate "text/template"
)

// renderSubject performs Go text/template substitution on a plain-text subject
// line. Variables are accessed as {{.VarName}} in the template string.
// Missing keys return an error so callers know the template is misconfigured.
func renderSubject(raw string, vars map[string]string) (string, error) {
	tmpl, err := texttemplate.New("subject").Option("missingkey=error").Parse(raw)
	if err != nil {
		return "", fmt.Errorf("render subject: parse: %w", err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, vars); err != nil {
		return "", fmt.Errorf("render subject: execute: %w", err)
	}
	return buf.String(), nil
}

// renderBody performs Go html/template substitution on an HTML body.
// html/template auto-escapes variable values which prevents XSS in email
// clients that render HTML content. Variables are accessed as {{.VarName}}.
func renderBody(raw string, vars map[string]string) (string, error) {
	tmpl, err := htmltemplate.New("body").Option("missingkey=error").Parse(raw)
	if err != nil {
		return "", fmt.Errorf("render body: parse: %w", err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, vars); err != nil {
		return "", fmt.Errorf("render body: execute: %w", err)
	}
	return buf.String(), nil
}
