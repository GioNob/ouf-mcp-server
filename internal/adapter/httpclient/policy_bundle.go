package httpclient

import (
	"context"
 "bytes"
 "crypto/sha256"
 "encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"

	"github.com/GioNob/ouf-mcp-server/internal/authorization"
)

type PolicyBundleClient struct {
	Endpoint      *url.URL
	Client        *http.Client
	WorkloadToken string
}

func NewPolicyBundle(endpoint, token string) (*PolicyBundleClient, error) {
	u, err := governed(endpoint)
	if err != nil {
		return nil, err
	}
	return &PolicyBundleClient{Endpoint: u, Client: sharedClient(), WorkloadToken: token}, nil
}

func (c *PolicyBundleClient) FetchActive(ctx context.Context) (authorization.ActivePolicyBundle, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.Endpoint.String(), nil)
	if err != nil {
		return authorization.ActivePolicyBundle{}, err
	}
	headers(req, c.WorkloadToken, "", "", "")
	res, err := c.Client.Do(req)
	if err != nil {
		return authorization.ActivePolicyBundle{}, err
	}
	defer res.Body.Close()
	if res.StatusCode/100 != 2 {
		return authorization.ActivePolicyBundle{}, errors.New("authorization policy bundle fetch rejected")
	}
 const maximum=5*1024*1024+65536
 raw,err:=io.ReadAll(io.LimitReader(res.Body,maximum+1));if err!=nil{return authorization.ActivePolicyBundle{},err}
 if len(raw)>maximum{return authorization.ActivePolicyBundle{},errors.New("authorization envelope too large")}
 var out authorization.ActivePolicyBundle
 decoder:=json.NewDecoder(bytes.NewReader(raw));decoder.DisallowUnknownFields()
 if err:=decoder.Decode(&out);err!=nil{return authorization.ActivePolicyBundle{},err}
 if err:=decoder.Decode(new(any));err!=io.EOF{return authorization.ActivePolicyBundle{},errors.New("trailing authorization content")}
 var envelope struct {Bundle json.RawMessage `json:"bundle"`};if err:=json.Unmarshal(raw,&envelope);err!=nil{return authorization.ActivePolicyBundle{},err}
 var compact bytes.Buffer;if err:=json.Compact(&compact,envelope.Bundle);err!=nil{return authorization.ActivePolicyBundle{},err}
 sum:=sha256.Sum256(compact.Bytes())
 if out.ContentHash!=hex.EncodeToString(sum[:]) {return authorization.ActivePolicyBundle{},errors.New("authorization bundle hash mismatch")}
 if err:=out.Validate();err!=nil{return authorization.ActivePolicyBundle{},err}
 return out,nil
}
