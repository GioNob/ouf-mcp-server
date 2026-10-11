package operational

import (
 "bytes"
 "context"
 "encoding/json"
 "strings"
 "time"
 "unicode/utf8"

 "github.com/google/uuid"
)

// Private composition only. Public adapters must authorize the root and verify
// delegated identity before calling it; owner positions never prove delivery.
type PublicationComposer struct {
 Cursor CursorComposer
 Bookmarks PublicationBookmarkStore
}
func canonicalPublicationFilters(raw []byte)(json.RawMessage,error){
 if len(raw)>4096{return nil,ErrCursorCapacity}
 if !utf8.Valid(raw){return nil,ErrCursorConflict}
 if _,err:=uniquePublicationJSON(raw);err!=nil{return nil,ErrCursorConflict}
 var fields map[string]json.RawMessage
 if json.Unmarshal(raw,&fields)!=nil||fields==nil{return nil,ErrCursorConflict}
 normalizedFields:=map[string]string{}
 for name,value:=range fields{
  var text string;if json.Unmarshal(value,&text)!=nil{return nil,ErrCursorConflict}
  switch name {
  case "sourceId":if strings.TrimSpace(text)==""||utf8.RuneCountInString(text)>200{return nil,ErrCursorConflict}
  case "state":if text!="OPEN"&&text!="RECOVERING"&&text!="RESOLVED"{return nil,ErrCursorConflict}
  default:return nil,ErrCursorConflict
  }
  normalizedFields[name]=text
 }
 normalized,err:=json.Marshal(normalizedFields);return normalized,err
}
func validatePublicationCapture(frame CursorFrame)error{
 if !frame.PublicationCoverage {
  if frame.PublicationParent!=nil||frame.PublicationBase!=nil{return ErrCursorConflict}
  for _,owner:=range frame.Owners{if owner.PublicationBookmark!=""||owner.PublicationParent!=""{return ErrCursorConflict}}
  return nil
 }
 filters,err:=canonicalPublicationFilters(frame.Filters);if err!=nil{return err}
 if (frame.PublicationParent==nil)!=(frame.PublicationBase==nil){return ErrCursorConflict}
 if frame.PublicationParent!=nil{
  if *frame.PublicationParent==uuid.Nil{return ErrCursorConflict}
  if _,err=CanonicalPublicationVector(*frame.PublicationBase);err!=nil{return err}
  baseFilters,err:=canonicalPublicationFilters(frame.PublicationBase.Filters)
  if err!=nil||!bytes.Equal(filters,baseFilters){return ErrCursorConflict}
 }
 for _,owner:=range frame.Owners{
  expected:=publicationOwnerParent(frame,owner.Name)
  if owner.PublicationBookmark!=""{
   if !canonicalPublicationToken(owner.PublicationBookmark)||owner.PublicationParent!=expected{return ErrCursorConflict}
  }else if owner.Done||owner.PublicationParent!=""{return ErrCursorConflict}
 }
 return nil
}
func publicationOwnerParent(frame CursorFrame,name string)string{
 if frame.PublicationBase!=nil{for _,owner:=range frame.PublicationBase.Owners{if owner.Name==name{return owner.Bookmark}}}
 return ""
}
func validatePublicationPage(page cursorOwnerPage,frame CursorFrame,name string)error{
 if page.BookmarkProfile!="ouf.owner-publication-bookmark.v1" || !canonicalPublicationToken(page.Cursor) || !canonicalPublicationToken(page.SnapshotRef) || page.NextCursor!=""&&!canonicalPublicationToken(page.NextCursor){return ErrCursorConflict}
 expected:=publicationOwnerParent(frame,name)
 if page.NextCursor==""{
  if !canonicalPublicationToken(page.PublicationBookmark)||page.Coverage.ParentBookmark!=expected||page.Coverage.NoPublicationProgress==nil{return ErrCursorConflict}
  unchanged:=expected!=""&&page.PublicationBookmark==expected
  if *page.Coverage.NoPublicationProgress!=unchanged{return ErrCursorConflict}
 }else if page.PublicationBookmark!=""||page.Coverage.ParentBookmark!=""||page.Coverage.NoPublicationProgress!=nil{return ErrCursorConflict}
 return nil
}
func(p PublicationComposer)configured()error{
 if p.Bookmarks==nil{return ErrCursorOwnerUnavailable}
 return p.Cursor.configured()
}
func(p PublicationComposer)Start(ctx context.Context,in aggregateRequest,policy string)([]byte,error){
 ctx,cancel:=context.WithTimeout(ctx,2500*time.Millisecond);defer cancel()
 if err:=p.configured();err!=nil{return nil,err}
 limit,valid:=operationalQuery(in.Arguments,true);if !valid{return nil,ErrCursorConflict}
 var fields map[string]json.RawMessage
 if json.Unmarshal(in.Arguments,&fields)!=nil{return nil,ErrCursorConflict};delete(fields,"limit")
 raw,_:=json.Marshal(fields);filters,err:=canonicalPublicationFilters(raw);if err!=nil{return nil,err}
 return p.capture(ctx,in,policy,filters,limit,nil,nil)
}
func(p PublicationComposer)Resume(ctx context.Context,in aggregateRequest,policy string,token uuid.UUID)([]byte,error){
 ctx,cancel:=context.WithTimeout(ctx,2500*time.Millisecond);defer cancel()
 if err:=p.configured();err!=nil{return nil,err}
 limit,valid:=operationalQuery(in.Arguments,true);if !valid{return nil,ErrCursorConflict}
 var fields map[string]json.RawMessage
 if json.Unmarshal(in.Arguments,&fields)!=nil||len(fields)>1{return nil,ErrCursorConflict}
 for key:=range fields{if key!="limit"{return nil,ErrCursorConflict}}
 binding,err:=BindPublicationBookmark(in.Identity,policy,p.Cursor.Aggregator.ManifestChecksum);if err!=nil{return nil,err}
 retained,err:=p.Bookmarks.LoadPublicationBookmark(ctx,token,binding);if err!=nil{return nil,err}
 if _,err=CanonicalPublicationVector(retained.Vector);err!=nil{return nil,err}
 in.ReadPositionRef=retained.Token.String()
 in.AttemptID=in.AttemptID+":publication-resume:"+uuid.NewString()
 return p.capture(ctx,in,policy,retained.Vector.Filters,limit,&retained.Token,&retained.Vector)
}
func(p PublicationComposer)capture(ctx context.Context,in aggregateRequest,policy string,filters json.RawMessage,limit int,parent *uuid.UUID,base *PublicationVector)([]byte,error){
 binding,err:=BindIncidentCursor(in.Identity,policy,p.Cursor.Aggregator.ManifestChecksum);if err!=nil{return nil,err}
 frame:=CursorFrame{Profile:CursorProfile,Filters:filters,PageLimit:limit,PublicationCoverage:true,PublicationParent:parent,PublicationBase:base}
 var scope map[string]json.RawMessage
 _=json.Unmarshal(filters,&scope)
 if source:=scope["sourceId"];source!=nil{_ = json.Unmarshal(source,&in.ScopeSource)}
 replies:=make(chan producerResult,2)
 for _,producer:=range incidentProducers{
  producer:=producer
  request:=in
  query:=map[string]any{"limit":100}
  if base==nil {
   var initial map[string]json.RawMessage;_=json.Unmarshal(filters,&initial)
   for key,value:=range initial{query[key]=value};query["snapshot"]=true;query["coverage"]=true
  }else{query["resumeFrom"]=publicationOwnerParent(frame,producer.Name)}
  request.Arguments,_=json.Marshal(query)
  go func(){body,ok:=p.Cursor.Aggregator.callProducer(ctx,request,producer);replies<-producerResult{Producer:producer,Body:body,OK:ok}}()
 }
 pages:=map[string]cursorOwnerPage{}
 for range 2{
  select{
  case reply:=<-replies:
   if !reply.OK{return nil,ErrCursorOwnerUnavailable}
   page,err:=decodeCursorOwnerPage(reply.Body,reply.Producer.Name);if err!=nil{return nil,err}
   if err=validatePublicationPage(page,frame,reply.Producer.Name);err!=nil{return nil,err}
   pages[reply.Producer.Name]=page
  case <-ctx.Done():return nil,ErrCursorOwnerUnavailable
  }
 }
 frame.SelfStatus,frame.SelfActionRequired,err=p.Cursor.currentSelf(ctx,in);if err!=nil{return nil,err}
 for _,name:=range []string{"GATEWAY","INGESTION"}{page,ok:=pages[name];if !ok{return nil,ErrCursorOwnerUnavailable};frame.Owners=append(frame.Owners,page.position(name,0))}
 if err=ValidateCursorFrame(frame);err!=nil{return nil,err}
 node,err:=p.Cursor.Store.CreateIncidentCursor(ctx,binding,frame);if err!=nil{return nil,err}
 return p.Cursor.compose(ctx,in,binding,node,pages,2,p.finalizer(ctx,in,policy))
}
func(p PublicationComposer)Page(ctx context.Context,in aggregateRequest,policy string,token uuid.UUID)([]byte,error){
 ctx,cancel:=context.WithTimeout(ctx,2500*time.Millisecond);defer cancel()
 if err:=p.configured();err!=nil{return nil,err}
 in.AttemptID=in.AttemptID+":publication-page:"+uuid.NewString();in.ScopeSource="";in.ReadPositionRef=""
 binding,err:=BindIncidentCursor(in.Identity,policy,p.Cursor.Aggregator.ManifestChecksum);if err!=nil{return nil,err}
 node,err:=p.Cursor.Store.LoadIncidentCursor(ctx,token,binding);if err!=nil{return nil,err}
 if err=ValidateCursorFrame(node.Frame);err!=nil{return nil,err}
 if !node.Frame.PublicationCoverage{return nil,ErrCursorConflict}
 if time.Now().After(node.ExpiresAt){return nil,ErrCursorExpired}
 in.ReadPositionRef=node.Token.String()
 pages:=map[string]cursorOwnerPage{}
 for _,owner:=range node.Frame.Owners{
  page,err:=p.Cursor.readPage(ctx,in,node.Frame.Filters,owner.Name,owner.Cursor,true);if err!=nil{return nil,err}
  if err=validatePublicationPage(page,node.Frame,owner.Name);err!=nil{return nil,err}
  if !sameOwnerPage(owner,page){return nil,ErrCursorConflict}
  pages[owner.Name]=page
 }
 status,action,err:=p.Cursor.currentSelf(ctx,in);if err!=nil{return nil,err}
 if status!=node.Frame.SelfStatus||action!=node.Frame.SelfActionRequired{return nil,ErrCursorConflict}
 return p.Cursor.compose(ctx,in,binding,node,pages,2,p.finalizer(ctx,in,policy))
}
func(p PublicationComposer)finalizer(ctx context.Context,in aggregateRequest,policy string)func(CursorFrame)(string,error){
 return func(frame CursorFrame)(string,error){
  if err:=ValidateCursorFrame(frame);err!=nil{return "",err}
  vector:=PublicationVector{Profile:PublicationVectorProfile,Filters:frame.Filters}
  for _,name:=range []string{"GATEWAY","INGESTION"}{
   for _,owner:=range frame.Owners{if owner.Name==name{
    if !owner.Done||owner.PublicationBookmark==""{return "",ErrCursorConflict}
    vector.Owners=append(vector.Owners,PublicationPosition{Name:name,Bookmark:owner.PublicationBookmark,ParentBookmark:owner.PublicationParent,SnapshotRef:owner.SnapshotRef,ItemsDigest:owner.OwnerDigest})
   }}
  }
  binding,err:=BindPublicationBookmark(in.Identity,policy,p.Cursor.Aggregator.ManifestChecksum);if err!=nil{return "",err}
  result,err:=p.Bookmarks.PutPublicationBookmark(ctx,binding,vector,frame.PublicationParent);if err!=nil{return "",err}
  return result.Token.String(),nil
 }
}
