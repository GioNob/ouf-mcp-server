package manifest

import (
 "encoding/json"
 "testing"
 "github.com/google/jsonschema-go/jsonschema"
)

// Some MCP clients generate argument types from union branches alone. Every
// branch must therefore retain the complete input contract, including closed
// properties and required reason/selector fields.
func TestPermissionUnionBranchesAreCompleteClientContracts(t *testing.T) {
 snapshot,err:=Load();if err!=nil {t.Fatal(err)}
 for _,c:=range snapshot.Capabilities {
  if c.CapabilityID!="authorization.permissions.read"&&c.CapabilityID!="authorization.permissions.propose"{continue}
  var schema map[string]any
  if err:=json.Unmarshal(c.InputSchema,&schema);err!=nil{t.Fatal(err)}
  var cases []string
  if c.CapabilityID=="authorization.permissions.read"{
   cases=[]string{`{"view":"ROLES"}`,`{"subjectId":"operator","limit":10}`,`{"externalRoleRef":"reviewers"}`}
  }else{
   cases=[]string{`{"operation":"REVOKE","grantId":"old","reason":"review"}`,`{"operation":"REPLACE_ROLES","reason":"review","roleCatalogue":{"issuer":"https://iam.example","roles":[],"assignments":[]}}`}
  }
  for _,input:=range cases {
   var value any;json.Unmarshal([]byte(input),&value)
   matched:=0
   for _,branch:=range schema["oneOf"].([]any){
    encoded,_:=json.Marshal(branch);var definition jsonschema.Schema
    if err:=json.Unmarshal(encoded,&definition);err!=nil{t.Fatal(err)}
    resolved,err:=definition.Resolve(nil);if err!=nil{t.Fatal(err)}
    if resolved.Validate(value)==nil{matched++}
   }
   if matched!=1{t.Fatalf("%s branch-only client input %s matches %d branches",c.CapabilityID,input,matched)}
  }
  var visit func(map[string]any)
  visit=func(node map[string]any){
   if alternatives,ok:=node["oneOf"].([]any);ok{
    for _,raw:=range alternatives{
     branch:=raw.(map[string]any)
     if _,ok:=node["properties"];ok{
      if branch["type"]!="object"||branch["additionalProperties"]!=false{t.Fatal("union branch loses closed object contract")}
      for key:=range node["properties"].(map[string]any){
       if _,ok:=branch["properties"].(map[string]any)[key];!ok{t.Fatalf("union branch loses field %s",key)}
      }
     }
    }
   }
   for _,value:=range node{
    switch x:=value.(type){
    case map[string]any:visit(x)
    case []any:for _,item:=range x{if obj,ok:=item.(map[string]any);ok{visit(obj)}}
    }
   }
  }
  visit(schema)
 }
}
