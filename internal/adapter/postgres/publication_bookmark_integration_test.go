package postgres

import(
 "context"
 "encoding/json"
 "errors"
 "os"
 "sync"
 "testing"

 "github.com/GioNob/ouf-mcp-server/internal/operational"
 "github.com/GioNob/ouf-mcp-server/internal/orchestration"
 "github.com/google/uuid"
)
func publicationFixture(t *testing.T)(*Store,operational.CursorBinding,operational.PublicationVector){
 t.Helper()
 store,old,frame:=cursorFixture(t)
 binding,err:=operational.BindPublicationBookmark(orchestration.Identity{PrincipalID:old.PrincipalID,TenantID:old.TenantID,ActorType:"HUMAN",Issuer:"issuer",Audience:"audience"},"policy:1",old.ManifestChecksum)
 if err!=nil{t.Fatal(err)}
 vector:=operational.PublicationVector{Profile:operational.PublicationVectorProfile,Filters:json.RawMessage(`{"sourceId":"source-a"}`)}
 for _,name:=range []string{"GATEWAY","INGESTION"}{
  for _,owner:=range frame.Owners{if owner.Name==name{vector.Owners=append(vector.Owners,operational.PublicationPosition{Name:name,Bookmark:uuid.NewString(),SnapshotRef:owner.SnapshotRef,ItemsDigest:owner.ItemsDigest})}}
 }
 return store,binding,vector
}
func publicationSuccessor(vector operational.PublicationVector)operational.PublicationVector{
 next:=vector;next.Owners=append([]operational.PublicationPosition{},vector.Owners...)
 for i:=range next.Owners{next.Owners[i].ParentBookmark=vector.Owners[i].Bookmark}
 return next
}
func TestPublicationBookmarkDurableStableIdentityAndPolicyScope(t *testing.T){
 store,binding,vector:=publicationFixture(t);ctx:=context.Background()
 first,err:=store.PutPublicationBookmark(ctx,binding,vector,nil);if err!=nil{t.Fatal(err)}
 reopened,err:=Open(ctx,os.Getenv("MCP_TEST_DATABASE_URL"));if err!=nil{t.Fatal(err)};defer reopened.Close()
 loaded,err:=reopened.LoadPublicationBookmark(ctx,first.Token,binding)
 if err!=nil||loaded.Token!=first.Token||loaded.Vector.Owners[0].Bookmark!=vector.Owners[0].Bookmark{t.Fatal("restart lost boundary",err)}
 for _,mutate:=range []func(*operational.CursorBinding){func(b *operational.CursorBinding){b.PrincipalID="other"},func(b *operational.CursorBinding){b.TenantID="other"},func(b *operational.CursorBinding){b.Fingerprint=hash64("other")}}{
  changed:=binding;mutate(&changed);if _,err=reopened.LoadPublicationBookmark(ctx,first.Token,changed);!errors.Is(err,operational.ErrCursorNotFound){t.Fatal("identity replay accepted",err)}
 }
 changed:=binding;changed.PolicyRef="policy:2"
 if _,err=reopened.LoadPublicationBookmark(ctx,first.Token,changed);!errors.Is(err,operational.ErrCursorConflict){t.Fatal("old policy retained",err)}
 changed=binding;changed.ManifestChecksum=hash64("changed")
 if _,err=reopened.LoadPublicationBookmark(ctx,first.Token,changed);!errors.Is(err,operational.ErrCursorConflict){t.Fatal("manifest drift accepted",err)}
}
func TestPublicationBookmarkLineageReplayIdleAndConcurrency(t *testing.T){
 store,binding,vector:=publicationFixture(t);ctx:=context.Background()
 first,err:=store.PutPublicationBookmark(ctx,binding,vector,nil);if err!=nil{t.Fatal(err)}
 idle:=publicationSuccessor(vector)
 for range 4{same,err:=store.PutPublicationBookmark(ctx,binding,idle,&first.Token);if err!=nil||same.Token!=first.Token{t.Fatal("idle grows retained positions",err)}}
 changed:=publicationSuccessor(vector);changed.Owners[0].Bookmark=uuid.NewString()
 forged:=publicationSuccessor(vector);forged.Owners[0].ParentBookmark=uuid.NewString()
 if _,err=store.PutPublicationBookmark(ctx,binding,forged,&first.Token);!errors.Is(err,operational.ErrCursorConflict){t.Fatal("unrelated owner lineage accepted",err)}
 var wg sync.WaitGroup;results:=make(chan operational.PublicationBookmark,8);failures:=make(chan error,8)
 for range 8{wg.Add(1);go func(){defer wg.Done();result,err:=store.PutPublicationBookmark(ctx,binding,changed,&first.Token);if err!=nil{failures<-err}else{results<-result}}()}
 wg.Wait();close(results);close(failures);var winner uuid.UUID
 for result:=range results{if winner!=uuid.Nil&&result.Token!=winner{t.Fatal("concurrent duplicate")};winner=result.Token}
 for err:=range failures{if !errors.Is(err,operational.ErrCursorCapacity){t.Fatal(err)}}
 if winner==uuid.Nil{t.Fatal("no committed successor")}
 replay,err:=store.PutPublicationBookmark(ctx,binding,changed,&first.Token)
 if err!=nil||replay.Token!=winner{t.Fatal("replay changed",err)}
 var count int
 if err=store.pool.QueryRow(ctx,"select count(*) from ouf_mcp.operational_publication_bookmark where tenant_id=$1",binding.TenantID).Scan(&count);err!=nil||count!=2{t.Fatal("idle/concurrent capacity leak",err,count)}
 if _,err=store.PutPublicationBookmark(ctx,binding,changed,nil);!errors.Is(err,operational.ErrCursorConflict){t.Fatal("lineage erased by new root",err)}
}
func TestPublicationBookmarkImmutableFiniteCapacityAndIndependentCursor(t *testing.T){
 store,binding,vector:=publicationFixture(t);ctx:=context.Background()
 first,err:=store.PutPublicationBookmark(ctx,binding,vector,nil);if err!=nil{t.Fatal(err)}
 for _,query:=range []string{"update ouf_mcp.operational_publication_bookmark set expires_at=expires_at+interval '1 day' where token=$1",
 "update ouf_mcp.operational_publication_bookmark set metadata=metadata||'{\"payload\":\"forged\"}'::jsonb where token=$1",
 "delete from ouf_mcp.operational_publication_bookmark where token=$1"}{
  if _,err=store.pool.Exec(ctx,query,first.Token);err==nil{t.Fatal("live metadata mutation accepted")}
 }
 _,err=store.pool.Exec(ctx,`insert into ouf_mcp.operational_publication_bookmark
 (token,parent_token,principal_id,tenant_id,identity_fingerprint,policy_ref,manifest_checksum,profile,scope_digest,vector_digest,metadata,created_at,expires_at)
 select gen_random_uuid(),parent_token,principal_id,tenant_id,identity_fingerprint,policy_ref,manifest_checksum,profile,scope_digest,vector_digest,metadata,created_at,expires_at
 from ouf_mcp.operational_publication_bookmark cross join generate_series(1,511) where token=$1`,first.Token)
 if err!=nil{t.Fatal(err)}
 next:=publicationSuccessor(vector);next.Owners[0].Bookmark=uuid.NewString()
 if _,err=store.PutPublicationBookmark(ctx,binding,next,&first.Token);!errors.Is(err,operational.ErrCursorCapacity){t.Fatal("unbounded retained positions",err)}
 replay,err:=store.PutPublicationBookmark(ctx,binding,vector,nil)
 if err!=nil||replay.Token!=first.Token{t.Fatal("capacity erased existing boundary",err)}
 // Publication capacity is separate from the temporary cursor/read budget.
 frame:=operational.CursorFrame{Profile:operational.CursorProfile,Filters:json.RawMessage(`{}`),PageLimit:1,SelfStatus:"HEALTHY"}
 for _,owner:=range vector.Owners{frame.Owners=append(frame.Owners,operational.OwnerPosition{Name:owner.Name,Cursor:uuid.NewString(),SnapshotRef:owner.SnapshotRef,ItemsDigest:owner.ItemsDigest,OwnerDigest:owner.ItemsDigest})}
 if _,err=store.CreateIncidentCursor(ctx,binding,frame);err!=nil{t.Fatal("bookmark capacity blocked independent cursor",err)}
}
func TestPublicationBookmarkExpiryGapAndBoundedCleanup(t *testing.T){
 store,binding,vector:=publicationFixture(t);ctx:=context.Background()
 first,err:=store.PutPublicationBookmark(ctx,binding,vector,nil);if err!=nil{t.Fatal(err)}
 expired:=uuid.New()
 _,err=store.pool.Exec(ctx,`insert into ouf_mcp.operational_publication_bookmark
 (token,principal_id,tenant_id,identity_fingerprint,policy_ref,manifest_checksum,profile,scope_digest,vector_digest,metadata,created_at,expires_at)
 select $2,principal_id,tenant_id,identity_fingerprint,policy_ref,manifest_checksum,profile,scope_digest,vector_digest,metadata,
 statement_timestamp()-interval '91 days',statement_timestamp()-interval '1 day'
 from ouf_mcp.operational_publication_bookmark where token=$1`,first.Token,expired)
 if err!=nil{t.Fatal(err)}
 if _,err=store.LoadPublicationBookmark(ctx,expired,binding);!errors.Is(err,operational.ErrCursorExpired){t.Fatal("expired gap became successful empty read",err)}
 next:=publicationSuccessor(vector);next.Owners[0].Bookmark=uuid.NewString()
 if _,err=store.PutPublicationBookmark(ctx,binding,next,&expired);!errors.Is(err,operational.ErrCursorExpired){t.Fatal("expired parent resumed",err)}
 if _,err=store.PutPublicationBookmark(ctx,binding,next,&first.Token);err!=nil{t.Fatal(err)}
 var exists bool
 if err=store.pool.QueryRow(ctx,"select exists(select 1 from ouf_mcp.operational_publication_bookmark where token=$1)",expired).Scan(&exists);err!=nil||exists{t.Fatal("expired cleanup failed",err)}
 if _,err=store.LoadPublicationBookmark(ctx,first.Token,binding);err!=nil{t.Fatal("cleanup removed live root",err)}
}
