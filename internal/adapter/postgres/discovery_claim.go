package postgres

import (
	"context"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// The exact hash includes the owner key. Older servers could admit several
// copies of the same owner command. Keep all rows and bind one actual attempt:
// an uncertain outcome takes precedence, otherwise the latest observation.
func existingDiscoveryClaim(ctx context.Context, tx pgx.Tx, in orchestration.AdmissionRequest) (uuid.UUID, string, error) {
	var id uuid.UUID
	var hash string
	err := tx.QueryRow(ctx, `select c.attempt_id,c.request_hash from ouf_mcp.idempotency_claim c
		join ouf_mcp.tool_attempt a using(attempt_id)
		where c.service_principal_id=$1 and c.principal_id=$2 and c.tenant_id=$3 and c.owner=$4
		and c.capability_id='ouf.semantic.discovery' and c.operation_class='COMMAND'
		and not c.batch_discovery_worker
		and (c.discovery_owner_key=$5 or (c.discovery_owner_key is null and c.request_hash=$6))
		order by (c.discovery_owner_key is not null) desc, (a.state in ('UNKNOWN','UNRESOLVED')) desc,
		a.admitted_at desc, c.attempt_id limit 1 for update of c`, in.Identity.ServicePrincipalID, in.Identity.PrincipalID, in.Identity.TenantID, in.Owner, in.IdempotencyKey, in.RequestHash).Scan(&id, &hash)
	if err != nil {
		return uuid.Nil, "", err
	}
	if hash == in.RequestHash {
		_, err = tx.Exec(ctx, `update ouf_mcp.idempotency_claim set discovery_owner_key=$2
			where attempt_id=$1 and discovery_owner_key is null`, id, in.IdempotencyKey)
	}
	return id, hash, err
}
