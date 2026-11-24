// Package maintenance exposes the two closed operations for administrative 019.
package maintenance

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"errors"
	"fmt"
	"strings"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed 019_analysis_run_retention.sql
var packagedSQL string

const pinnedSHA256 = "7c7d26ece7fad21ab79cb59a5fdf740dc1a49a38026ff10f20255180472a3b0d"

func SQLDigest() string { return fmt.Sprintf("%x", sha256.Sum256([]byte(packagedSQL))) }

var errUnverified = errors.New("analysis retention unverified")

type Counts struct {
	Reads     int `json:"reads"`
	Snapshots int `json:"snapshots"`
	Attempts  int `json:"attempts"`
}

// Fingerprint both signatures from live catalogs using stable role names.
// Compare each installed body to its dollar-quoted source in the pinned asset.
// Four edges means exactly the owner and maintenance may execute each function.
const inspectACL = `
with functions(signature, delimiter) as (values
 ('ouf_mcp.purge_analysis_run_history(text,timestamptz,timestamptz,timestamptz)', '$purge$'),
 ('ouf_mcp.purge_analysis_run_history(text,timestamptz,timestamptz,timestamptz,integer)', '$batch$'))
select encode(sha256(convert_to(string_agg(concat_ws('|', f.signature, owner.rolname,
  grantor.rolname, grantee.rolname, a.privilege_type, a.is_grantable), E'\n'
  order by f.signature collate "C", grantee.rolname::text collate "C"), 'UTF8')), 'hex')
from functions f
join pg_proc p on p.oid=to_regprocedure(f.signature)
join pg_roles owner on owner.oid=p.proowner
join pg_language l on l.oid=p.prolang
cross join lateral aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a
left join pg_roles grantee on grantee.oid=a.grantee
join pg_roles grantor on grantor.oid=a.grantor
having count(*)=4 and count(distinct p.oid)=2 and bool_and((
 owner.rolname='ouf_mcp_retention_owner' and not owner.rolcanlogin and not owner.rolsuper
 and not owner.rolcreaterole and not exists(select 1 from pg_auth_members m where m.roleid=owner.oid)
 and p.prosecdef and l.lanname='plpgsql' and p.provolatile='v' and not p.proisstrict
 and p.proconfig=array['search_path=pg_catalog, pg_temp'] and p.prosrc=split_part($1, f.delimiter, 2)
 and a.privilege_type='EXECUTE' and not a.is_grantable and a.grantor=p.proowner
 and a.grantee in (p.proowner,'ouf_mcp_maintenance_role'::regrole)) is true)`

func Install(ctx context.Context, pool *pgxpool.Pool, databaseOID uint32, appRole string) (string, error) {
	// Keep the asset directly applicable by psql. In the command the outer
	// transaction also covers target validation and the final catalog read.
	body, begin := strings.CutPrefix(packagedSQL, "begin;\n")
	body, end := strings.CutSuffix(body, "commit;\n")
	if !begin || !end || SQLDigest() != pinnedSHA256 || databaseOID == 0 || appRole == "" {
		return "", errUnverified
	}
	var acl string
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "set local search_path=pg_catalog,pg_temp"); err != nil {
			return err
		}
		var target bool
		err := tx.QueryRow(ctx, `select d.oid=$1 and r.rolname<>current_user and not r.rolsuper and not r.rolcreaterole
			from pg_database d cross join pg_roles r where d.datname=current_database() and r.rolname::text=$2`,
			databaseOID, appRole).Scan(&target)
		if err != nil || !target {
			return errUnverified
		}
		if _, err := tx.Exec(ctx, body); err != nil {
			return err
		}
		var appExcluded bool
		err = tx.QueryRow(ctx, `select not has_function_privilege($1::name,
			'ouf_mcp.purge_analysis_run_history(text,timestamptz,timestamptz,timestamptz,integer)','EXECUTE')`,
			appRole).Scan(&appExcluded)
		if err != nil || !appExcluded {
			return errUnverified
		}
		return tx.QueryRow(ctx, inspectACL, packagedSQL).Scan(&acl)
	})
	if err != nil {
		return "", err
	}
	return acl, nil
}

const MaxBatch = 256

func Once(ctx context.Context, pool *pgxpool.Pool, workload string, ttl analysis.RunRetention, limit int) (string, Counts, error) {
	if SQLDigest() != pinnedSHA256 || workload == "" || limit < 1 || limit > MaxBatch {
		return "", Counts{}, errUnverified
	}
	var acl string
	var counts Counts
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `set local search_path=pg_catalog,pg_temp;
			set local lock_timeout='3s'; set local statement_timeout='10s';
			select pg_advisory_xact_lock_shared(73012026)`); err != nil {
			return err
		}
		var maintenance bool
		if err := tx.QueryRow(ctx, `select current_user='ouf_mcp_maintenance_role'
			and not has_table_privilege(current_user,'ouf_mcp.analysis_run_read','DELETE')
			and not has_table_privilege(current_user,'ouf_mcp.analysis_run_snapshot','DELETE')
			and not has_table_privilege(current_user,'ouf_mcp.analysis_run_attempt','DELETE')`).Scan(&maintenance); err != nil || !maintenance {
			return errUnverified
		}
		if err := tx.QueryRow(ctx, inspectACL, packagedSQL).Scan(&acl); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `select reads,snapshots,attempts from ouf_mcp.purge_analysis_run_history($1,
			transaction_timestamp()-$2::interval,transaction_timestamp()-$3::interval,
			transaction_timestamp()-$4::interval,$5)`, workload, max(ttl.Reservations, ttl.Attempts),
			ttl.Snapshots, ttl.Reads, limit).Scan(&counts.Reads, &counts.Snapshots, &counts.Attempts)
	})
	if err != nil {
		return "", Counts{}, err
	}
	return acl, counts, nil
}
