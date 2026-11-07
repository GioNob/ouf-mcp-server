package kernel

import (
 "context"
 "encoding/json"

 "github.com/GioNob/ouf-mcp-server/internal/analysis"
 "github.com/GioNob/ouf-mcp-server/internal/manifest"
 "github.com/GioNob/ouf-mcp-server/internal/orchestration"
 "github.com/google/jsonschema-go/jsonschema"
 "github.com/modelcontextprotocol/go-sdk/mcp"
)

func registerAnalysisTool(server *mcp.Server,c manifest.Capability,snapshot *manifest.Snapshot,service *orchestration.Service){
 var schema jsonschema.Schema
 if e:=json.Unmarshal(c.InputSchema,&schema);e!=nil{panic(e)}
 mcp.AddTool(server,&mcp.Tool{Name:c.ToolName,Description:c.Description(),InputSchema:&schema,Annotations:&mcp.ToolAnnotations{ReadOnlyHint:c.ToolName=="source.analysis.read"}},func(ctx context.Context,_ *mcp.CallToolRequest,input map[string]any)(*mcp.CallToolResult,any,error){
  i,ok:=ctx.Value(identityKey{}).(requestIdentity);if !ok||i.Delegation==""||i.ActorType!="HUMAN"{return errorResult("UNAUTHENTICATED",false),nil,nil}
  store,ok:=service.Admission.(analysis.Store);if !ok{return errorResult("ANALYSIS_STORE_NOT_BOUND",false),nil,nil}
  coordinator:=analysis.Coordinator{Store:store,Service:service,Manifest:snapshot}
  raw,_:=json.Marshal(input);var out any;var err error
  switch c.ToolName{
  case "source.analysis.plan":
   var args struct{AssetID string `json:"assetId"`;ProfileID string `json:"profileId"`;ExpectedRevision int64 `json:"expectedRevision"`;Work []analysis.Work `json:"work"`}
   if json.Unmarshal(raw,&args)!=nil{return errorResult("ANALYSIS_INPUT_INVALID",false),nil,nil}
   out,err=coordinator.Plan(ctx,i.Identity,args.AssetID,args.ProfileID,args.ExpectedRevision,args.Work)
  case "source.analysis.read":
   var args struct{AnalysisID string `json:"analysisId"`;Offset int `json:"offset"`;Limit int `json:"limit"`}
   if json.Unmarshal(raw,&args)!=nil{return errorResult("ANALYSIS_INPUT_INVALID",false),nil,nil};if args.Limit==0{args.Limit=128}
   out,err=coordinator.Read(ctx,i.Identity,args.AnalysisID,args.Offset,args.Limit)
  case "source.analysis.step":
   var args struct{AnalysisID string `json:"analysisId"`;TaskID string `json:"taskId"`}
   if json.Unmarshal(raw,&args)!=nil{return errorResult("ANALYSIS_INPUT_INVALID",false),nil,nil}
   out,err=coordinator.Step(ctx,i.Identity,args.AnalysisID,args.TaskID)
  default:return errorResult("ANALYSIS_INPUT_INVALID",false),nil,nil
  }
  if err!=nil{return errorResult(analysis.SafeError(err),false),nil,nil}
  body,err:=json.Marshal(out);if err!=nil||len(body)>1<<20{return errorResult("ANALYSIS_RESULT_LIMIT",false),nil,nil}
  return &mcp.CallToolResult{Content:[]mcp.Content{&mcp.TextContent{Text:string(body)}}},nil,nil
 })
}
