package operational

import (
 "encoding/json"
 "errors"
 "net/http"
 "unicode/utf8"

 "github.com/GioNob/ouf-mcp-server/internal/orchestration"
 "github.com/google/uuid"
)

// Publication positions are READ metadata, never delivery or consultation receipts.
func (h *ownerAPI) incidentPublication(w http.ResponseWriter, r *http.Request, identity orchestration.Identity, raw []byte) bool {
 mode, query, token, valid := publicationHTTPQuery(raw)
 if !valid { http.Error(w,"invalid operational request",400); return true }
 if mode == "" { return false }
 if identity.Delegation == "" { http.Error(w,"verified aggregate delegated context required",403); return true }
 if h.cursorStore == nil || h.publicationStore == nil { http.Error(w,"operational publication unavailable",503); return true }
 in := aggregateRequest{Identity:identity,Arguments:query,AttemptID:r.Header.Get("X-Tool-Attempt-ID"),CorrelationID:r.Header.Get("X-Correlation-ID")}
 composer := PublicationComposer{Cursor:CursorComposer{Aggregator:h.aggregator,Store:h.cursorStore},Bookmarks:h.publicationStore}
 policy := r.Header.Get("X-OUF-Authorization-Decision-Ref")
 var body []byte
 var err error
 switch mode {
 case "start": body,err=composer.Start(r.Context(),in,policy)
 case "page": body,err=composer.Page(r.Context(),in,policy,token)
 case "resume": body,err=composer.Resume(r.Context(),in,policy,token)
 }
 if err != nil {
  status,message := 503,"operational publication unavailable"
  switch {
  case errors.Is(err,ErrCursorNotFound): status,message=404,"operational publication not found"
  case errors.Is(err,ErrCursorExpired): status,message=410,"operational publication history gap"
  case errors.Is(err,ErrCursorConflict): status,message=409,"operational publication invalidated"
  case errors.Is(err,ErrCursorCapacity): status,message=429,"operational publication capacity unavailable"
  }
  http.Error(w,message,status); return true
 }
 if len(body)>512<<10 || !json.Valid(body) { http.Error(w,"operational result unavailable",503);return true }
 w.Header().Set("Content-Type","application/json")
 w.WriteHeader(200)
 _,_=w.Write(body)
 return true
}

// Closed modes are initial coverage, coverage page, or inherited-filter resume.
func publicationHTTPQuery(raw []byte) (string,json.RawMessage,uuid.UUID,bool) {
 invalid := func()(string,json.RawMessage,uuid.UUID,bool){return "",nil,uuid.Nil,false}
 if !utf8.Valid(raw) { return invalid() }
 if _,err:=uniquePublicationJSON(raw);err!=nil {return invalid()}
 var fields map[string]json.RawMessage
 if json.Unmarshal(raw,&fields)!=nil || fields==nil {return invalid()}
 _,coverage:=fields["coverage"]
 _,resume:=fields["resumeFrom"]
 if !coverage && !resume {return "",nil,uuid.Nil,true}
 tokenFrom:=func(value json.RawMessage)(uuid.UUID,bool){
  var text string
  if json.Unmarshal(value,&text)!=nil || !canonicalPublicationToken(text){return uuid.Nil,false}
  return uuid.MustParse(text),true
 }
 if resume {
  token,ok:=tokenFrom(fields["resumeFrom"])
  if !ok || coverage {return invalid()}
  delete(fields,"resumeFrom")
  for name:=range fields {if name!="limit" {return invalid()}}
  query,_:=json.Marshal(fields)
  if _,valid:=operationalQuery(query,true);!valid{return invalid()}
  return "resume",query,token,true
 }
 var flag *bool
 if json.Unmarshal(fields["coverage"],&flag)!=nil || flag==nil || !*flag {return invalid()}
 delete(fields,"coverage")
 if value,exists:=fields["cursor"];exists {
  token,ok:=tokenFrom(value)
  if !ok || len(fields)!=1 {return invalid()}
  return "page",json.RawMessage("{}"),token,true
 }
 if json.Unmarshal(fields["snapshot"],&flag)!=nil || flag==nil || !*flag {return invalid()}
 delete(fields,"snapshot")
 query,_:=json.Marshal(fields)
 if _,valid:=operationalQuery(query,true);!valid{return invalid()}
 delete(fields,"limit")
 filters,_:=json.Marshal(fields)
 if _,err:=canonicalPublicationFilters(filters);err!=nil{return invalid()}
 return "start",query,uuid.Nil,true
}

