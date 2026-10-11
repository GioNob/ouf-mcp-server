package postgres

import (
 "context"
 "crypto/sha256"
 "encoding/hex"
 "encoding/json"
 "errors"
 "time"

 "github.com/GioNob/ouf-mcp-server/internal/operational"
 "github.com/google/uuid"
 "github.com/jackc/pgx/v5"
)

type publicationQuerier interface {
 QueryRow(context.Context,string,...any) pgx.Row
}
func publicationDigest(raw []byte)string{sum:=sha256.Sum256(raw);return hex.EncodeToString(sum[:])}
func publicationHashes(vector operational.PublicationVector)(string,string,error){
 raw,err:=operational.CanonicalPublicationVector(vector);if err!=nil{return "","",err}
 normalized,err:=operational.DecodePublicationVector(raw);if err!=nil{return "","",err}
 positions:=[]string{normalized.Owners[0].Bookmark,normalized.Owners[1].Bookmark}
 boundaries,_:=json.Marshal(positions)
 return publicationDigest(normalized.Filters),publicationDigest(boundaries),nil
}

func loadPublicationBookmark(ctx context.Context,q publicationQuerier,token uuid.UUID,binding operational.CursorBinding)(operational.PublicationBookmark,error){
 result:=operational.PublicationBookmark{Token:token};var raw []byte;var policy,manifest string;var valid bool
 err:=q.QueryRow(ctx,`select parent_token,metadata,expires_at,policy_ref,manifest_checksum,expires_at>clock_timestamp()
 from ouf_mcp.operational_publication_bookmark where token=$1 and principal_id=$2 and tenant_id=$3 and identity_fingerprint=$4`,
 token,binding.PrincipalID,binding.TenantID,binding.Fingerprint).Scan(&result.Parent,&raw,&result.ExpiresAt,&policy,&manifest,&valid)
 if errors.Is(err,pgx.ErrNoRows){return result,operational.ErrCursorNotFound};if err!=nil{return result,err}
 if policy!=binding.PolicyRef || manifest!=binding.ManifestChecksum{return result,operational.ErrCursorConflict}
 if !valid{return result,operational.ErrCursorExpired}
 result.Vector,err=operational.DecodePublicationVector(raw)
 return result,err
}
func(s *Store) LoadPublicationBookmark(ctx context.Context,token uuid.UUID,binding operational.CursorBinding)(operational.PublicationBookmark,error){
 if !validCursorBinding(binding)||token==uuid.Nil{return operational.PublicationBookmark{},operational.ErrCursorNotFound}
 ctx,cancel:=context.WithTimeout(ctx,2*time.Second);defer cancel()
 return loadPublicationBookmark(ctx,s.pool,token,binding)
}

func(s *Store) PutPublicationBookmark(ctx context.Context,binding operational.CursorBinding,vector operational.PublicationVector,parent *uuid.UUID)(operational.PublicationBookmark,error){
 empty:=operational.PublicationBookmark{}
 if !validCursorBinding(binding)||parent!=nil&&*parent==uuid.Nil{return empty,operational.ErrCursorConflict}
 raw,err:=operational.CanonicalPublicationVector(vector);if err!=nil{return empty,err}
 normalized,err:=operational.DecodePublicationVector(raw);if err!=nil{return empty,err}
 scopeDigest,vectorDigest,err:=publicationHashes(normalized);if err!=nil{return empty,err}
 ctx,cancel:=context.WithTimeout(ctx,2*time.Second);defer cancel()
 tx,err:=s.pool.Begin(ctx);if err!=nil{return empty,err};defer func(){_ = tx.Rollback(ctx)}()
 var locked bool
 if err=tx.QueryRow(ctx,"select pg_try_advisory_xact_lock(73012028009)").Scan(&locked);err!=nil{return empty,err}
 if !locked{return empty,operational.ErrCursorCapacity}
 if parent!=nil{
  old,loadErr:=loadPublicationBookmark(ctx,tx,*parent,binding);if loadErr!=nil{return empty,loadErr}
  if !operational.PublicationVectorLineage(old.Vector,normalized){return empty,operational.ErrCursorConflict}
  if operational.SamePublicationBoundaries(old.Vector,normalized){return old,tx.Commit(ctx)}
 }else{
  for _,position:=range normalized.Owners{if position.ParentBookmark!=""{return empty,operational.ErrCursorConflict}}
 }
 // Same boundaries on the same parent are idempotent, including concurrent
 // independent page chains. An equivalent live token is never replaced.
 var token uuid.UUID
 err=tx.QueryRow(ctx,`select token from ouf_mcp.operational_publication_bookmark
 where principal_id=$1 and tenant_id=$2 and identity_fingerprint=$3 and policy_ref=$4 and manifest_checksum=$5
 and scope_digest=$6 and vector_digest=$7 and parent_token is not distinct from $8::uuid
 and expires_at>clock_timestamp() order by created_at,token limit 1`,
 binding.PrincipalID,binding.TenantID,binding.Fingerprint,binding.PolicyRef,binding.ManifestChecksum,scopeDigest,vectorDigest,parent).Scan(&token)
 if err==nil{old,loadErr:=loadPublicationBookmark(ctx,tx,token,binding);if loadErr!=nil{return empty,loadErr};return old,tx.Commit(ctx)}
 if !errors.Is(err,pgx.ErrNoRows){return empty,err}
 _,err=tx.Exec(ctx,`delete from ouf_mcp.operational_publication_bookmark where token in
 (select token from ouf_mcp.operational_publication_bookmark where expires_at<=clock_timestamp() order by expires_at limit 1 for update skip locked)`)
 if err!=nil{return empty,err}
 var total,tenant,principal int
 err=tx.QueryRow(ctx,`select count(*),count(*) filter(where tenant_id=$1),count(*) filter(where tenant_id=$1 and principal_id=$2)
 from ouf_mcp.operational_publication_bookmark`,binding.TenantID,binding.PrincipalID).Scan(&total,&tenant,&principal)
 if err!=nil{return empty,err}
 if total>=32768||tenant>=4096||principal>=512{return empty,operational.ErrCursorCapacity}
 result:=operational.PublicationBookmark{Token:uuid.New(),Parent:parent,Vector:normalized}
 err=tx.QueryRow(ctx,`insert into ouf_mcp.operational_publication_bookmark
 (token,parent_token,principal_id,tenant_id,identity_fingerprint,policy_ref,manifest_checksum,profile,scope_digest,vector_digest,metadata)
 values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb) returning expires_at`,
 result.Token,parent,binding.PrincipalID,binding.TenantID,binding.Fingerprint,binding.PolicyRef,binding.ManifestChecksum,
 operational.PublicationVectorProfile,scopeDigest,vectorDigest,string(raw)).Scan(&result.ExpiresAt)
 if err!=nil{return empty,err}
 return result,tx.Commit(ctx)
}
