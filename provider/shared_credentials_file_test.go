package provider

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const exampleRole = "arn:aws:iam::123456789012:role/example-role"

// webIdentityFileSetup recreates what HCP Terraform dynamic provider credentials provide:
// only AWS_SHARED_CREDENTIALS_FILE is set, pointing at a file whose [default] profile has
// role_arn, web_identity_token_file and role_session_name.
func webIdentityFileSetup(t *testing.T) {
	dir := t.TempDir()
	tok := filepath.Join(dir, "web-identity-token")
	if err := os.WriteFile(tok, []byte("dummy.jwt.token"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := filepath.Join(dir, "shared-credentials")
	content := fmt.Sprintf("[default]\nrole_arn=%s\nweb_identity_token_file=%s\nrole_session_name=session-test\n", exampleRole, tok)
	if err := os.WriteFile(cfg, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE",
		"AWS_CONFIG_FILE", "AWS_SDK_LOAD_CONFIG", "AWS_ROLE_ARN", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_SESSION_NAME"} {
		t.Setenv(k, "")
	}
	t.Setenv("HOME", dir)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", cfg)
}

type captureSTS struct{ body string }

func (c *captureSTS) RoundTrip(r *http.Request) (*http.Response, error) {
	b, _ := io.ReadAll(r.Body)
	c.body = r.URL.Host + " " + string(b)
	xml := `<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><AssumeRoleWithWebIdentityResult><Credentials><AccessKeyId>ASIAFAKEKEY</AccessKeyId><SecretAccessKey>fakesecret</SecretAccessKey><SessionToken>faketoken</SessionToken><Expiration>2099-01-01T00:00:00Z</Expiration></Credentials><AssumedRoleUser><AssumedRoleId>X:session-test</AssumedRoleId><Arn>arn:aws:sts::1:assumed-role/r/session-test</Arn></AssumedRoleUser></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>`
	return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/xml"}},
		Body: io.NopCloser(strings.NewReader(xml)), Request: r}, nil
}

// TestSharedCredentialsFileWebIdentity: with validate=false (as in providers/gsi.tf) and the TFC file layout,
// the client must resolve credentials via AssumeRoleWithWebIdentity for the file's role.
func TestSharedCredentialsFileWebIdentity(t *testing.T) {
	webIdentityFileSetup(t)
	cap := &captureSTS{}
	orig := http.DefaultClient.Transport
	http.DefaultClient.Transport = cap
	defer func() { http.DefaultClient.Transport = orig }()

	c, err := newClient("us-west-2", "", "", "", "", "", "", false)
	if err != nil {
		t.Fatalf("newClient error: %v", err)
	}
	v, err := c.Config.Credentials.Get()
	if err != nil {
		t.Fatalf("credentials error: %v", err)
	}
	t.Logf("resolved AccessKeyID=%s provider=%s", v.AccessKeyID, v.ProviderName)
	t.Logf("STS request: %s", cap.body)
	if !strings.Contains(cap.body, "RoleArn=") || !strings.Contains(cap.body, "example-role") {
		t.Fatalf("STS request did not carry the role ARN from the file")
	}
	if !strings.Contains(cap.body, "WebIdentityToken=dummy.jwt.token") {
		t.Fatalf("STS request did not carry the token from the file")
	}
}

// Existing behaviour must not change: no credentials and validate=true is still an error.
func TestNoCredentialsStillErrors(t *testing.T) {
	for _, k := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE",
		"AWS_CONFIG_FILE", "AWS_SDK_LOAD_CONFIG", "AWS_ROLE_ARN", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_SHARED_CREDENTIALS_FILE"} {
		t.Setenv(k, "")
	}
	t.Setenv("HOME", t.TempDir())
	if _, err := newClient("us-west-2", "", "", "", "", "", "", true); err == nil || !strings.Contains(err.Error(), "no credentials for AWS") {
		t.Fatalf("expected 'no credentials for AWS', got %v", err)
	}
}

// Explicit keys still win over the shared file.
func TestExplicitKeysStillWin(t *testing.T) {
	webIdentityFileSetup(t)
	c, err := newClient("us-west-2", "AKIAEXPLICIT", "secret", "", "", "", "", true)
	if err != nil {
		t.Fatal(err)
	}
	v, err := c.Config.Credentials.Get()
	if err != nil || v.AccessKeyID != "AKIAEXPLICIT" {
		t.Fatalf("expected explicit keys, got %+v err=%v", v, err)
	}
}
