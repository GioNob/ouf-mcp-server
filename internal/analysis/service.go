package analysis

import (
 "context"
 "encoding/json"
 "errors"
 "math/rand/v2"
 "net/http"
 "strconv"
 "time"

 "github.com/GioNob/ouf-mcp-server/internal/manifest"
 "github.com/GioNob/ouf-mcp-server/internal/orchestration"
 "github.com/google/uuid"
)

type Coordinator struct { Store Store; Service *orchestration.Service; Manifest *manifest.Snapshot }
func (c Coordinator) authorize(ctx context.Context,i orchestration.Identity,cap manifest.Capability)error{
 if i.Delegation==""||i.ActorType!="HUMAN"||!IdentityScope(i).Valid(){return orchestration.ErrUnauthorized}
 d,e:=c.Service.Auth.Authorize(ctx,orchestration.AuthorizationRequest{Identity:i,Resource:orchestration.ResourceContext{ResourceType:"capability",TenantID:i.TenantID},CapabilityID:cap.RequiredAuthorization,Owner:cap.Owner,OperationClass:cap.OperationClass})
 if e!=nil{return e};if !d.Allowed||d.DecisionRef==""{return orchestration.ErrUnauthorized};return nil
}
func (c Coordinator) check(ctx context.Context,i orchestration.Identity)(string,error){
 preview,ok:=Find(c.Manifest,"source.file.preview");if !ok||preview.PublicationState!="ACTIVE"{return "",ErrUnavailable}
 if e:=c.authorize(ctx,i,preview);e!=nil{return "",e};return c.Manifest.Checksum()
}
func (c Coordinator) Plan(ctx context.Context,i orchestration.Identity,asset,profile string,expected int64,work []Work)(View,error){
 checksum,e:=c.check(ctx,i);if e!=nil{return View{},e}
 if uuid.Validate(asset)!=nil||uuid.Validate(profile)!=nil||len(work)<1||len(work)>MaxPlanBatch{return View{},ErrInvalid}
 preview,_:=Find(c.Manifest,"source.file.preview")
 args,_:=json.Marshal(map[string]any{"assetId":asset,"profileId":profile,"view":"COLUMNS","offset":0,"limit":1})
 r,e:=c.Service.Call(ctx,orchestration.Invocation{Identity:i,CapabilityID:preview.CapabilityID,Owner:preview.Owner,OperationClass:"READ",GatewayBindingRef:preview.GatewayBindingRef,ManifestChecksum:checksum,Arguments:args,IdempotencyKey:uuid.NewString(),CorrelationID:uuid.NewString(),Window:time.Minute,Timeout:10*time.Second,RetryThreshold:3,Maximum:orchestration.Cost{ToolCalls:1,DistinctObjects:1,ResultBytes:1<<20}})
 if e!=nil{return View{},e};if r.Replay||r.Problem!=nil{return View{},ErrScope}
 var p Profile;var envelope struct{View string `json:"view"`;Offset int `json:"offset"`;Columns []json.RawMessage `json:"columns"`}
 if json.Unmarshal(r.Body,&p)!=nil||json.Unmarshal(r.Body,&envelope)!=nil||p.AssetID!=asset||p.ProfileID!=profile||!p.Valid()||envelope.View!="COLUMNS"||envelope.Offset!=0||len(envelope.Columns)!=1{return View{},ErrScope}
 plan,e:=Build(IdentityScope(i),p,c.Manifest,expected,work);if e!=nil{return View{},e};return c.Store.PlanAnalysis(ctx,plan)
}
func (c Coordinator) Read(ctx context.Context,i orchestration.Identity,id string,offset,limit int)(View,error){
 checksum,e:=c.check(ctx,i);if e!=nil{return View{},e};return c.Store.ReadAnalysis(ctx,IdentityScope(i),id,checksum,offset,limit)
}
func (c Coordinator) Step(ctx context.Context,i orchestration.Identity,id,taskID string)(Task,error){
 checksum,e:=c.check(ctx,i);if e!=nil{return Task{},e}
 if ValidateID(id)!=nil||ValidateID(taskID)!=nil{return Task{},ErrInvalid}
 scope:=IdentityScope(i)
 metadata,e:=c.Store.ReadAnalysisTask(ctx,scope,id,taskID,checksum);if e!=nil{return Task{},e}
 cap,ok:=Find(c.Manifest,metadata.Tool);if !ok||!allowed(metadata.Tool)||cap.PublicationState!="ACTIVE"{return Task{},ErrUnavailable}
 // A previously saved result is historical evidence. Even its reread requires
 // current authorization for the underlying semantic capability.
 if e=c.authorize(ctx,i,cap);e!=nil{return Task{},e}
 task,e:=c.Store.ClaimAnalysisTask(ctx,scope,id,taskID,checksum);if e!=nil{return Task{},e}
 if task.State!="RUNNING"||task.Fence==""{return task,nil}
 start:=time.Now()
 r,callErr:=c.Service.Call(ctx,orchestration.Invocation{Identity:i,CapabilityID:cap.CapabilityID,Owner:cap.Owner,OperationClass:cap.OperationClass,GatewayBindingRef:cap.GatewayBindingRef,ManifestChecksum:checksum,Arguments:task.Arguments,IdempotencyKey:"analysis:"+Digest([]any{id,task.ID,task.Attempts}),CorrelationID:uuid.NewString(),Window:time.Minute,Timeout:time.Duration(task.MaxTimeMS)*time.Millisecond,RetryThreshold:3,Maximum:orchestration.Cost{ToolCalls:1,DistinctObjects:task.MaxObjects,ResultBytes:task.MaxBytes}})
 f:=Finish{State:"HELD",Code:"OUTCOME_REQUIRES_RECONCILIATION",ElapsedMS:time.Since(start).Milliseconds()}
 if r.AttemptID!=uuid.Nil{f.AttemptID=r.AttemptID.String()}
 var window interface{ BudgetWindowEnd() time.Time }
 if errors.As(callErr,&window){
  f.State="WAITING";f.Code="ADMISSION_WINDOW_WAIT";f.NotBefore=window.BudgetWindowEnd().Add(time.Second);f.WindowWait=true;f.ElapsedMS=0
 }else if callErr!=nil{
  if errors.Is(callErr,orchestration.ErrUnauthorized){f.Code="AUTHORIZATION_DENIED"}
 }else if r.Replay{
  f.Code="OWNER_RESULT_RECONCILIATION_REQUIRED"
 }else if r.Problem!=nil{
  f.Calls=1;f.Code="OWNER_REQUEST_REJECTED"
  if cap.OperationClass=="READ"&&(r.Problem.Status==429||r.Problem.Status==503){
   delay:=[]time.Duration{5*time.Second,15*time.Second,time.Minute,5*time.Minute,15*time.Minute}[min(task.Attempts-1,4)]
   delay+=time.Duration(rand.Int64N(int64(delay)/5+1))
   f.NotBefore=time.Now().Add(delay)
   if requested:=retryAfter(r.Problem.RetryAfter,time.Now());requested.After(f.NotBefore){f.NotBefore=requested}
   if task.Attempts<MaxRetryAttempts&&f.NotBefore.Before(task.FirstAttemptAt.Add(24*time.Hour)){f.State="WAITING";f.Code="TRANSIENT_OWNER_WAIT"}else{f.Code="RETRY_BUDGET_EXHAUSTED"}
  }
 }else if len(r.Body)>0&&len(r.Body)<=int(task.MaxBytes)&&json.Valid(r.Body){
  f.State="SUCCEEDED";f.Code="EVIDENCE_COLLECTED_NOT_ADOPTED";f.Calls=1;f.Body=r.Body
 }
 // Reconciliation has its own bounded context. A disconnected MCP client must
 // not erase an already observed outcome; a failed write leaves a fenced hold.
 finishCtx,cancel:=context.WithTimeout(context.WithoutCancel(ctx),2*time.Second);defer cancel()
 return c.Store.FinishAnalysisTask(finishCtx,scope,id,taskID,task.Fence,f)
}
func retryAfter(value string,now time.Time)time.Time{
 if n,e:=strconv.ParseInt(value,10,64);e==nil&&n>=0&&n<=86400{return now.Add(time.Duration(n)*time.Second)}
 if at,e:=http.ParseTime(value);e==nil{return at};return time.Time{}
}
