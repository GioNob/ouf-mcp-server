package postgres

import (
 "context"
 "crypto/sha256"
 "encoding/hex"
 "errors"
 "io/fs"
 "regexp"
 "strconv"
 "strings"
 "time"

 "github.com/jackc/pgx/v5"
 "github.com/jackc/pgx/v5/pgxpool"
)

var ErrReprocessingMigrationUnproven = errors.New("MCP_REPROCESSING_MIGRATION_UNPROVEN")

// MigrateSemanticReprocessing runs only the packaged 12->13 transition through
// an explicitly selected administrative channel. The sealed infrastructure
// owner authenticates its finite HUMAN mandate before invoking this primitive.
// Membership exists solely inside the same transaction as the DDL and ledger:
// no other connection can see it, and cancellation/rollback cannot leave it.
// A failed commit response is not proof of rollback: read migration 13 back.
func MigrateSemanticReprocessing(ctx context.Context, pool *pgxpool.Pool, applicationRole string, databaseOID uint32, functionHash string) error {
 deny:=func()error{return ErrReprocessingMigrationUnproven}
 if !regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`).MatchString(applicationRole) || databaseOID==0 || !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(functionHash){return deny()}
 entries,e:=fs.ReadDir(migrationFiles,"migrations");if e!=nil || len(entries)!=13{return deny()}
 expected:=map[int]string{};var body13 []byte
 for _,entry:=range entries {
  if entry.IsDir(){return deny()};parts:=strings.SplitN(entry.Name(),"_",2);if len(parts)!=2 || !strings.HasSuffix(entry.Name(),".sql"){return deny()}
  version,e:=strconv.Atoi(parts[0]);if e!=nil || version<1 || version>13{return deny()}
  raw,e:=migrationFiles.ReadFile("migrations/"+entry.Name());if e!=nil{return deny()};hash:=sha256.Sum256(raw);expected[version]=hex.EncodeToString(hash[:]);if version==13{body13=raw}
 }
 if len(expected)!=13 || len(body13)==0{return deny()}
 ctx,cancel:=context.WithTimeout(ctx,25*time.Second);defer cancel()
 tx,e:=pool.BeginTx(ctx,pgx.TxOptions{IsoLevel:pgx.Serializable});if e!=nil{return deny()}
 defer func(){rollbackCtx,stop:=context.WithTimeout(context.Background(),2*time.Second);defer stop();_=tx.Rollback(rollbackCtx)}()
 if _,e=tx.Exec(ctx,"set local statement_timeout='10s'; set local lock_timeout='3s'; select pg_advisory_xact_lock(73012026)");e!=nil{return deny()}
 var oid uint32;var admin,app,retention,owner bool;var members int64;var observedFunctionHash string
 e=tx.QueryRow(ctx,`select d.oid,r.rolsuper,
 exists(select 1 from pg_roles where rolname=$1 and not rolsuper and not rolcreaterole),
 exists(select 1 from pg_roles where rolname='ouf_mcp_retention_owner' and not rolcanlogin and not rolsuper and not rolcreaterole),
 (select count(*) from pg_auth_members where roleid=(select oid from pg_roles where rolname='ouf_mcp_retention_owner')),
 coalesce((select p.proowner=(select oid from pg_roles where rolname='ouf_mcp_retention_owner') from pg_proc p where p.oid=to_regprocedure('ouf_mcp.purge_terminal_attempt_graph(timestamp with time zone,integer)')),false),
 (select encode(sha256(convert_to(pg_get_functiondef(p.oid),'UTF8')),'hex') from pg_proc p where p.oid=to_regprocedure('ouf_mcp.purge_terminal_attempt_graph(timestamp with time zone,integer)'))
 from pg_database d cross join pg_roles r where d.datname=current_database() and r.rolname=current_user`,applicationRole).Scan(&oid,&admin,&app,&retention,&members,&owner,&observedFunctionHash)
 if e!=nil || oid!=databaseOID || !admin || !app || !retention || members!=0 || !owner || observedFunctionHash!=functionHash{return deny()}
 rows,e:=tx.Query(ctx,"select version,checksum from ouf_mcp.schema_migration order by version");if e!=nil{return deny()}
 count:=0
 for rows.Next(){var version int;var checksum string;if e=rows.Scan(&version,&checksum);e!=nil{rows.Close();return deny()};count++;if version!=count || version>12 || checksum!=expected[version]{rows.Close();return deny()}}
 rows.Close();if rows.Err()!=nil || count!=12{return deny()}
 appIdent:=pgx.Identifier{applicationRole}.Sanitize()
 if _,e=tx.Exec(ctx,"grant ouf_mcp_retention_owner to "+appIdent+" with admin false, inherit true, set true");e!=nil{return deny()}
 if _,e=tx.Exec(ctx,"set local role "+appIdent);e!=nil{return deny()}
 if _,e=tx.Exec(ctx,string(body13));e!=nil{return deny()}
 if _,e=tx.Exec(ctx,"insert into ouf_mcp.schema_migration(version,checksum) values(13,$1)",expected[13]);e!=nil{return deny()}
 if _,e=tx.Exec(ctx,"reset role; revoke ouf_mcp_retention_owner from "+appIdent);e!=nil{return deny()}
 if e=tx.QueryRow(ctx,"select count(*) from pg_auth_members where roleid=(select oid from pg_roles where rolname='ouf_mcp_retention_owner')").Scan(&members);e!=nil || members!=0{return deny()}
 if e=tx.Commit(ctx);e!=nil{return deny()}
 return nil
}
