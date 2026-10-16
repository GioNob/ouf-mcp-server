// Package reprocessing verifies an installation-bound, dual-signed, one-call
// admission session. It never authorizes a semantic approval or owner decision.
package reprocessing

import (
 "bytes"
 "crypto/ed25519"
 "crypto/sha256"
 "encoding/hex"
 "encoding/json"
 "errors"
 "io"
 "os"
 "path/filepath"
 "strings"
 "syscall"
 "time"

 "github.com/google/uuid"
)

var ErrPermit = errors.New("SEMANTIC_REPROCESSING_PERMIT_INVALID")
var ErrExpired = errors.New("SEMANTIC_REPROCESSING_SESSION_EXPIRED")

// KeyProfile is trusted deployment configuration, hash-bound separately from
// the untrusted permit. Keys must be the existing installation role keys.
type KeyProfile struct {
 Schema string `json:"schema"`
 InstallationRef string `json:"installationRef"`
 InstallerKeyRef string `json:"installerKeyRef"`
 AttestorKeyRef string `json:"attestorKeyRef"`
 InstallerPublicKey []byte `json:"installerPublicKey"`
 AttestorPublicKey []byte `json:"attestorPublicKey"`
 NotBefore int64 `json:"notBefore"`
 ExpiresAt int64 `json:"expiresAt"`
}

type Permit struct {
 Schema string `json:"schema"`
 SessionID string `json:"sessionId"`
 DecisionRef string `json:"decisionRef"`
 Reason string `json:"reason"`
 InstallationRef string `json:"installationRef"`
 KeyProfileHash string `json:"keyProfileHash"`
 ManifestChecksum string `json:"manifestChecksum"`
 ServicePrincipalID string `json:"servicePrincipalId"`
 HumanSubject string `json:"humanSubject"`
 TenantID string `json:"tenantId"`
 Owner string `json:"owner"`
 CapabilityID string `json:"capabilityId"`
 OperationClass string `json:"operationClass"`
 CommandKey string `json:"commandKey"`
 RequestHash string `json:"requestHash"`
 PredecessorAttemptID string `json:"predecessorAttemptId"`
 PredecessorLockVersion int64 `json:"predecessorLockVersion"`
 PredecessorOutcomeCode string `json:"predecessorOutcomeCode"`
 PriorToolCalls int64 `json:"priorToolCalls"`
 MaximumNewToolCalls int64 `json:"maximumNewToolCalls"`
 MaximumCumulativeToolCalls int64 `json:"maximumCumulativeToolCalls"`
 OwnerReceiptAbsentAt int64 `json:"ownerReceiptAbsentAt"`
 IssuedAt int64 `json:"issuedAt"`
 ExpiresAt int64 `json:"expiresAt"`
}

type Envelope struct {
 Payload []byte `json:"payload"`
 InstallerSignature []byte `json:"installerSignature"`
 AttestorSignature []byte `json:"attestorSignature"`
}

type VerifiedPermit struct { value Permit; hash string }
func (v *VerifiedPermit) Value() Permit { return v.value }
func (v *VerifiedPermit) Hash() string { return v.hash }
func Hash(raw []byte) string { sum:=sha256.Sum256(raw);return hex.EncodeToString(sum[:]) }
func hash(s string) bool { _,e:=hex.DecodeString(s);return len(s)==64&&e==nil&&strings.ToLower(s)==s }
func bounded(s string,n int) bool { return len(s)>0&&len(s)<=n&&!strings.ContainsAny(s,"\x00\r\n") }

func decode(raw []byte,v any) error {
 if len(raw)==0||len(raw)>65536{return ErrPermit}
 // Duplicate keys are rejected at every nesting level before typed decoding.
 d:=json.NewDecoder(bytes.NewReader(raw));if err:=uniqueValue(d);err!=nil{return ErrPermit}
 if _,err:=d.Token();err!=io.EOF{return ErrPermit}
 d=json.NewDecoder(bytes.NewReader(raw));d.DisallowUnknownFields()
 if d.Decode(v)!=nil{return ErrPermit};return nil
}
func uniqueValue(d *json.Decoder) error {
 t,e:=d.Token();if e!=nil{return e};delim,ok:=t.(json.Delim);if !ok{return nil}
 if delim=='{' { keys:=map[string]bool{};for d.More(){k,e:=d.Token();if e!=nil{return e};s,ok:=k.(string);if !ok||keys[s]{return ErrPermit};keys[s]=true;if e=uniqueValue(d);e!=nil{return e}} } else if delim=='[' {for d.More(){if e=uniqueValue(d);e!=nil{return e}}} else{return ErrPermit}
 _,e=d.Token();return e
}

// RoleFrame is domain-separated from every other use of the existing keys.
func RoleFrame(role string,payload []byte) []byte {
 return append([]byte("ouf.mcp-admission-reprocessing.v1\n"+role+"\n"),payload...)
}
func Verify(profileRaw,envelopeRaw []byte,expectedProfileHash,manifest string,now time.Time)(*VerifiedPermit,error){
 if !hash(expectedProfileHash)||Hash(profileRaw)!=expectedProfileHash||!hash(manifest){return nil,ErrPermit}
 var profile KeyProfile;var env Envelope;var p Permit
 if decode(profileRaw,&profile)!=nil||decode(envelopeRaw,&env)!=nil||decode(env.Payload,&p)!=nil{return nil,ErrPermit}
 if profile.Schema!="ouf.mcp-reprocessing-key-profile.v1"||!bounded(profile.InstallationRef,128)||!bounded(profile.InstallerKeyRef,256)||!bounded(profile.AttestorKeyRef,256)||profile.InstallerKeyRef==profile.AttestorKeyRef||len(profile.InstallerPublicKey)!=ed25519.PublicKeySize||len(profile.AttestorPublicKey)!=ed25519.PublicKeySize||bytes.Equal(profile.InstallerPublicKey,profile.AttestorPublicKey){return nil,ErrPermit}
 if !ed25519.Verify(profile.InstallerPublicKey,RoleFrame("DEPLOYMENT_INTENT",env.Payload),env.InstallerSignature)||!ed25519.Verify(profile.AttestorPublicKey,RoleFrame("FINAL_DEPLOYMENT_APPROVAL",env.Payload),env.AttestorSignature){return nil,ErrPermit}
 id,err:=uuid.Parse(p.SessionID);prior,e:=uuid.Parse(p.PredecessorAttemptID)
 if err!=nil||e!=nil||id==uuid.Nil||prior==uuid.Nil||p.Schema!="ouf.mcp-semantic-reprocessing-session.v1"||!bounded(p.DecisionRef,256)||!bounded(p.Reason,512)||p.InstallationRef!=profile.InstallationRef||p.KeyProfileHash!=expectedProfileHash||p.ManifestChecksum!=manifest||!bounded(p.ServicePrincipalID,128)||!bounded(p.HumanSubject,128)||!bounded(p.TenantID,128)||p.Owner!="semantic"||p.CapabilityID!="ouf.semantic.propose"||p.OperationClass!="COMMAND"||!bounded(p.CommandKey,128)||!hash(p.RequestHash)||p.PredecessorLockVersion<0||p.PredecessorOutcomeCode!="SEM_CAPABILITY_REQUIRED:ouf.semantic.propose"||p.PriorToolCalls!=1||p.MaximumNewToolCalls!=1||p.MaximumCumulativeToolCalls!=2||p.ExpiresAt-p.IssuedAt<=0||p.ExpiresAt-p.IssuedAt>900||p.OwnerReceiptAbsentAt>p.IssuedAt||p.OwnerReceiptAbsentAt<p.IssuedAt-300||p.IssuedAt<profile.NotBefore||p.ExpiresAt>profile.ExpiresAt{return nil,ErrPermit}
 verified:=&VerifiedPermit{value:p,hash:Hash(env.Payload)}
 if err=verified.CheckTime(now,0);err!=nil{return verified,err};return verified,nil
}
func(v *VerifiedPermit) CheckTime(now time.Time,remaining time.Duration)error{
 if v==nil{return ErrPermit};n:=now.Unix();if n<v.value.IssuedAt||!now.Add(remaining).Before(time.Unix(v.value.ExpiresAt,0)){return ErrExpired};return nil
}
func(v *VerifiedPermit) Matches(service,subject,tenant,capability,owner,operation,key string)bool{
 if v==nil{return false};p:=v.value
 return p.ServicePrincipalID==service&&p.HumanSubject==subject&&p.TenantID==tenant&&p.CapabilityID==capability&&p.Owner==owner&&p.OperationClass==operation&&p.CommandKey==key
}

// ReadDeploymentFile has no symlink/ancestry fallback. Both files are public
// metadata in read-only mounts; no private signing key or bearer is opened.
func ReadDeploymentFile(name string)([]byte,error){
 if !filepath.IsAbs(name)||filepath.Clean(name)!=name{return nil,ErrPermit}
 for p:=filepath.Dir(name);;p=filepath.Dir(p){s,e:=os.Lstat(p);if e!=nil||!s.IsDir()||s.Mode().Perm()&0022!=0{return nil,ErrPermit};u,ok:=s.Sys().(*syscall.Stat_t);if !ok||u.Uid!=0{return nil,ErrPermit};if p=="/"{break}}
 fd,e:=syscall.Open(name,syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK,0);if e!=nil{return nil,ErrPermit}
 f:=os.NewFile(uintptr(fd),name);defer f.Close();before,e:=f.Stat();if e!=nil||!before.Mode().IsRegular()||before.Mode().Perm()&0022!=0||before.Size()<1||before.Size()>65536{return nil,ErrPermit}
 u,ok:=before.Sys().(*syscall.Stat_t);if !ok||u.Uid!=0||u.Nlink!=1{return nil,ErrPermit}
 raw,e:=io.ReadAll(io.LimitReader(f,65537));after,e2:=f.Stat();path,e3:=os.Lstat(name)
 if e!=nil||e2!=nil||e3!=nil||!os.SameFile(before,after)||!os.SameFile(before,path)||before.Size()!=int64(len(raw))||before.Size()!=after.Size()||!before.ModTime().Equal(after.ModTime()){return nil,ErrPermit};return raw,nil
}
