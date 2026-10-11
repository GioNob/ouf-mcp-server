package operational

import (
 "encoding/json"
 "strings"
 "testing"
 "time"

 "github.com/GioNob/ouf-mcp-server/internal/orchestration"
 "github.com/google/uuid"
)
func publicationTestVector()PublicationVector{
 return PublicationVector{Profile:PublicationVectorProfile,Filters:json.RawMessage(`{"sourceId":"source","state":"OPEN"}`),Owners:[]PublicationPosition{
  {Name:"GATEWAY",Bookmark:uuid.NewString(),SnapshotRef:uuid.NewString(),ItemsDigest:strings.Repeat("a",64)},
  {Name:"INGESTION",Bookmark:uuid.NewString(),SnapshotRef:uuid.NewString(),ItemsDigest:strings.Repeat("b",64)},
 }}
}
func TestPublicationBookmarkBindingSurvivesLoginWithoutPreservingGrant(t *testing.T){
 identity:=orchestration.Identity{PrincipalID:"operator",TenantID:"tenant",ActorType:"HUMAN",ServicePrincipalID:"mcp",Issuer:"issuer",Audience:"audience",AuthenticationContextRef:"login-one",Scopes:[]string{"operations.incident.read"}}
 first,err:=BindPublicationBookmark(identity,"policy:1",strings.Repeat("c",64));if err!=nil{t.Fatal(err)}
 identity.AuthenticationContextRef="login-two";identity.Scopes=nil
 identity.Claims=&orchestration.IdentityClaims{AuthenticatedAt:time.Now()}
 second,err:=BindPublicationBookmark(identity,"policy:1",strings.Repeat("c",64))
 if err!=nil||first!=second{t.Fatal("login incorrectly replaced stable position")}
 for _,mutate:=range []func(*orchestration.Identity){
  func(i *orchestration.Identity){i.PrincipalID="other"},func(i *orchestration.Identity){i.TenantID="other"},
  func(i *orchestration.Identity){i.ActorType="SERVICE"},func(i *orchestration.Identity){i.ServicePrincipalID="other"},
  func(i *orchestration.Identity){i.Issuer="other"},func(i *orchestration.Identity){i.Audience="other"},
 }{
  changed:=identity;mutate(&changed);binding,err:=BindPublicationBookmark(changed,"policy:1",strings.Repeat("c",64))
  if err!=nil||binding.Fingerprint==first.Fingerprint{t.Fatal("stable identity field lost")}
 }
 if _,err:=BindPublicationBookmark(orchestration.Identity{},"policy:1",strings.Repeat("c",64));err==nil{t.Fatal("unverified identity accepted")}
}
func TestPublicationBookmarkMetadataIsClosedCanonicalAndPayloadFree(t *testing.T){
 vector:=publicationTestVector();raw,err:=CanonicalPublicationVector(vector);if err!=nil{t.Fatal(err)}
 if _,err=DecodePublicationVector(raw);err!=nil{t.Fatal(err)}
 for _,mutate:=range []func(*PublicationVector){
  func(v *PublicationVector){v.Profile="other"},func(v *PublicationVector){v.Filters=json.RawMessage(`{"since":"2026-01-01T00:00:00Z"}`)},
  func(v *PublicationVector){v.Filters=json.RawMessage(`{"sourceId":"source","sourceId":"other"}`)},
  func(v *PublicationVector){v.Filters=json.RawMessage(`{"state":null}`)},
  func(v *PublicationVector){v.Owners[0].Bookmark="00000000-0000-0000-0000-000000000000"},
  func(v *PublicationVector){v.Owners[0].Bookmark="11111111-AAAA-4111-8111-111111111111"},
  func(v *PublicationVector){v.Owners[1].Name="GATEWAY"},func(v *PublicationVector){v.Owners[0].ItemsDigest="short"},
 }{
  changed:=vector;changed.Owners=append([]PublicationPosition{},vector.Owners...);mutate(&changed)
  if _,err=CanonicalPublicationVector(changed);err==nil{t.Fatal("unproven metadata accepted")}
 }
 for _,suffix:=range []string{",\"payload\":{\"incident\":\"secret\"}",",\"consultationRecorded\":true",",\"profile\":\""+PublicationVectorProfile+"\""}{
  forged:=append([]byte{},raw[:len(raw)-1]...);forged=append(forged,[]byte(suffix+"}")...)
  if _,err=DecodePublicationVector(forged);err==nil{t.Fatal("extra or duplicate metadata accepted")}
 }
 if _,err=DecodePublicationVector(append(raw,[]byte("{}")...));err==nil{t.Fatal("trailing payload accepted")}
}
func TestPublicationBookmarkLineageUsesOwnerEqualityNeverUUIDOrder(t *testing.T){
 previous:=publicationTestVector();next:=previous;next.Owners=append([]PublicationPosition{},previous.Owners...)
 for i:=range next.Owners{next.Owners[i].ParentBookmark=previous.Owners[i].Bookmark}
 if !PublicationVectorLineage(previous,next)||!SamePublicationBoundaries(previous,next){t.Fatal("idle boundary changed")}
 next.Owners[0].Bookmark="00000001-0000-4000-8000-000000000001"
 if !PublicationVectorLineage(previous,next)||SamePublicationBoundaries(previous,next){t.Fatal("UUID order used instead of lineage")}
 next.Owners[1].ParentBookmark=uuid.NewString()
 if PublicationVectorLineage(previous,next){t.Fatal("skipped owner parent accepted")}
 next.Owners[1].ParentBookmark=previous.Owners[1].Bookmark;next.Filters=json.RawMessage(`{"state":"RESOLVED"}`)
 if PublicationVectorLineage(previous,next){t.Fatal("changed scope accepted")}
}
