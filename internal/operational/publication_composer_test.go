package operational

import (
 "context"
 "encoding/json"
 "errors"
 "sync"
 "testing"
 "time"

 "github.com/google/uuid"
)
type publicationStoreFixture struct{
 mu sync.Mutex
 records map[uuid.UUID]PublicationBookmark
 bindings map[uuid.UUID]CursorBinding
 failure error
 writes int
}
func(s *publicationStoreFixture)PutPublicationBookmark(_ context.Context,binding CursorBinding,vector PublicationVector,parent *uuid.UUID)(PublicationBookmark,error){
 s.mu.Lock();defer s.mu.Unlock()
 if s.failure!=nil{return PublicationBookmark{},s.failure}
 if _,err:=CanonicalPublicationVector(vector);err!=nil{return PublicationBookmark{},err}
 if s.records==nil{s.records=map[uuid.UUID]PublicationBookmark{};s.bindings=map[uuid.UUID]CursorBinding{}}
 if parent!=nil {
  old,ok:=s.records[*parent];if !ok||s.bindings[*parent]!=binding{return PublicationBookmark{},ErrCursorNotFound}
  if !PublicationVectorLineage(old.Vector,vector){return PublicationBookmark{},ErrCursorConflict}
  if SamePublicationBoundaries(old.Vector,vector){return old,nil}
 }
 raw,_:=CanonicalPublicationVector(vector)
 for token,old:=range s.records{
  existing,_:=CanonicalPublicationVector(old.Vector)
  if s.bindings[token]==binding&&string(existing)==string(raw){return old,nil}
 }
 retained:=PublicationBookmark{Token:uuid.New(),Parent:parent,Vector:vector,ExpiresAt:time.Now().Add(90*24*time.Hour)}
 s.records[retained.Token]=retained;s.bindings[retained.Token]=binding;s.writes++;return retained,nil
}
func(s *publicationStoreFixture)LoadPublicationBookmark(_ context.Context,token uuid.UUID,binding CursorBinding)(PublicationBookmark,error){
 s.mu.Lock();defer s.mu.Unlock()
 result,ok:=s.records[token];if !ok||s.bindings[token]!=binding{return PublicationBookmark{},ErrCursorNotFound}
 if time.Now().After(result.ExpiresAt){return PublicationBookmark{},ErrCursorExpired}
 return result,nil
}
func publicationComposerFixture(limit int)(PublicationComposer,aggregateRequest,*composerCallerFixture,*composerStoreFixture,*publicationStoreFixture,map[string]string){
 cursor,in,caller,store:=composerFixture(limit);book:=&publicationStoreFixture{}
 tokens:=map[string]string{"GATEWAY":uuid.NewString(),"INGESTION":uuid.NewString()}
 caller.transform=func(name string,page *cursorOwnerPage){
  page.BookmarkProfile="ouf.owner-publication-bookmark.v1"
  if page.NextCursor==""{page.PublicationBookmark=tokens[name];noProgress:=false;page.Coverage.NoPublicationProgress=&noProgress}
 }
 return PublicationComposer{Cursor:cursor,Bookmarks:book},in,caller,store,book,tokens
}
func publicationResult(t *testing.T,body []byte,err error)map[string]any{
 t.Helper();if err!=nil{t.Fatal(err)};var result map[string]any
 if json.Unmarshal(body,&result)!=nil{t.Fatal(string(body))};return result
}
func TestPublicationComposerCompleteChainAndReplayDoNotRecordConsultation(t *testing.T){
 p,in,caller,_,book,_:=publicationComposerFixture(1)
 body,err:=p.Start(context.Background(),in,"policy:1");first:=publicationResult(t,body,err);page:=first
 if _,ok:=page["publicationBookmark"];ok||book.writes!=0{t.Fatal("unseen root chain finalized early")}
 for page["nextCursor"]!=nil{
  body,err=p.Page(context.Background(),in,"policy:1",uuid.MustParse(page["nextCursor"].(string)))
  page=publicationResult(t,body,err)
 }
 if page["publicationBookmark"]==nil||book.writes!=1||page["consultationRecorded"]!=false{t.Fatal("missing read position or implicit consultation")}
 coverage:=page["coverage"].(map[string]any)
 if coverage["mcpSelfHistoryCovered"]!=false{t.Fatal("invented MCP history coverage")}
 final,err:=p.Page(context.Background(),in,"policy:1",uuid.MustParse(page["cursor"].(string)))
 if err!=nil||string(final)!=string(body)||book.writes!=1{t.Fatal("unstable final replay",err)}
 for _,call:=range caller.calls{
  var query map[string]any;_=json.Unmarshal(call.Arguments,&query)
  if query["coverage"]!=true{t.Fatal("owner read lost coverage",query)}
  if call.Owner=="ingestion"&&call.Resource.Attributes["sourceRef"]!="source-a"{t.Fatal("lost saved source grant")}
  if call.Owner=="gateway"&&call.Resource.Attributes["incidentScope"]!="PLATFORM"{t.Fatal("widened gateway scope")}
 }
}
func TestPublicationComposerResumeAfterNewLoginUsesExactOwnerLineage(t *testing.T){
 p,in,caller,store,book,original:=publicationComposerFixture(100)
 body,err:=p.Start(context.Background(),in,"policy:1");first:=publicationResult(t,body,err)
 token:=uuid.MustParse(first["publicationBookmark"].(string))
 caller.pages=map[string][]cursorOwnerPage{"GATEWAY":{composerPage("GATEWAY","WARNING",100,0,false)},"INGESTION":{composerPage("INGESTION","ERROR",100,1,false)}}
 successor:=map[string]string{"GATEWAY":original["GATEWAY"],"INGESTION":uuid.NewString()}
 caller.transform=func(name string,page *cursorOwnerPage){
  page.BookmarkProfile="ouf.owner-publication-bookmark.v1";page.PublicationBookmark=successor[name]
  page.Coverage.ParentBookmark=original[name];idle:=successor[name]==original[name];page.Coverage.NoPublicationProgress=&idle
 }
 in.Identity.AuthenticationContextRef="new-login";in.AttemptID=uuid.NewString();in.Arguments=json.RawMessage(`{"limit":100}`)
 // Temporary page sessions may expire; the durable vector is independent.
 for key,node:=range store.nodes{node.ExpiresAt=time.Now().Add(-time.Minute);store.nodes[key]=node}
 body,err=p.Resume(context.Background(),in,"policy:1",token);resumed:=publicationResult(t,body,err)
 if len(resumed["items"].([]any))!=1||resumed["publicationBookmark"]==token.String()||book.writes!=2{t.Fatal("resume lost changed publication")}
 for _,call:=range caller.calls[len(caller.calls)-2:]{
  var query map[string]any;_=json.Unmarshal(call.Arguments,&query)
  name:="INGESTION";if call.Owner=="gateway"{name="GATEWAY"}
  if query["resumeFrom"]!=original[name]||len(query)!=2||call.ReadPositionRef!=token.String(){t.Fatal("incorrect opaque owner boundary",query)}
  if call.Owner=="ingestion"&&call.Resource.Attributes["sourceRef"]!="source-a"{t.Fatal("resume failed inherited source SDK binding")}
 }
}
func TestPublicationComposerFailureAndMetadataCapacityNeverAdvance(t *testing.T){
 p,in,caller,store,book,_:=publicationComposerFixture(1)
 body,err:=p.Start(context.Background(),in,"policy:1");first:=publicationResult(t,body,err)
 next:=uuid.MustParse(first["nextCursor"].(string));before:=store.commits
 caller.fail="INGESTION"
 if _,err=p.Page(context.Background(),in,"policy:1",next);!errors.Is(err,ErrCursorOwnerUnavailable){t.Fatal("owner outage became successful empty page",err)}
 if store.commits!=before||book.writes!=0{t.Fatal("outage advanced metadata")}
 caller.fail=""
 caller.transform=func(name string,page *cursorOwnerPage){page.BookmarkProfile="wrong"}
 if _,err=p.Page(context.Background(),in,"policy:1",next);!errors.Is(err,ErrCursorConflict){t.Fatal("unproven owner coverage accepted",err)}
 if store.commits!=before||book.writes!=0{t.Fatal("mutation advanced")}
 p,in,_,store,book,_=publicationComposerFixture(100);book.failure=ErrCursorCapacity
 if _,err=p.Start(context.Background(),in,"policy:1");!errors.Is(err,ErrCursorCapacity){t.Fatal("metadata capacity ignored",err)}
 if store.commits!=0||book.writes!=0{t.Fatal("capacity advanced final cursor")}
}
func TestPublicationComposerClosedFiltersAndIdentityCannotBypassOwnerProof(t *testing.T){
 p,in,caller,_,book,_:=publicationComposerFixture(100)
 for _,raw:=range []string{`{"since":"2026-10-11T00:00:00Z"}`,`{"sourceId":null}`,`{"sourceId":"source","sourceId":"other"}`,`{"revision":1}`}{
  in.Arguments=json.RawMessage(raw)
  if _,err:=p.Start(context.Background(),in,"policy:1");err==nil{t.Fatal("unproven start filters accepted",raw)}
 }
 if len(caller.calls)!=0||book.writes!=0{t.Fatal("malformed input caused owner I/O")}
 in.Arguments=json.RawMessage(`{"limit":100,"sourceId":"source-a"}`)
 body,err:=p.Start(context.Background(),in,"policy:1");first:=publicationResult(t,body,err);token:=uuid.MustParse(first["publicationBookmark"].(string));before:=len(caller.calls)
 in.Identity.PrincipalID="other";in.Arguments=json.RawMessage(`{}`)
 if _,err=p.Resume(context.Background(),in,"policy:1",token);!errors.Is(err,ErrCursorNotFound){t.Fatal("principal replay accepted",err)}
 in.Identity.PrincipalID="operator";in.Arguments=json.RawMessage(`{"state":"RESOLVED"}`)
 if _,err=p.Resume(context.Background(),in,"policy:1",token);!errors.Is(err,ErrCursorConflict){t.Fatal("resume changed saved scope",err)}
 if len(caller.calls)!=before{t.Fatal("invalid resume contacted owners")}
}
func TestPublicationComposerRequiresConsistentOwnerNoProgressAndParent(t *testing.T){
 p,in,caller,_,book,_:=publicationComposerFixture(100)
 caller.transform=func(name string,page *cursorOwnerPage){
  page.BookmarkProfile="ouf.owner-publication-bookmark.v1";page.PublicationBookmark=uuid.NewString()
  yes:=true;page.Coverage.NoPublicationProgress=&yes
 }
 if _,err:=p.Start(context.Background(),in,"policy:1");!errors.Is(err,ErrCursorConflict){t.Fatal("initial coverage claimed unchanged parent",err)}
 if book.writes!=0{t.Fatal("unproven boundary issued")}
 p,in,caller,_,book,_=publicationComposerFixture(100)
 caller.transform=func(name string,page *cursorOwnerPage){
  page.BookmarkProfile="ouf.owner-publication-bookmark.v1";page.PublicationBookmark=uuid.NewString()
  page.Coverage.ParentBookmark=uuid.NewString();no:=false;page.Coverage.NoPublicationProgress=&no
 }
 if _,err:=p.Start(context.Background(),in,"policy:1");!errors.Is(err,ErrCursorConflict){t.Fatal("forged initial lineage accepted",err)}
}
