package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

type selfIncident struct {
	IncidentKey string    `json:"incidentRef"`
	Category    string    `json:"category"`
	DetailCode  string    `json:"detailCode"`
	CreatedAt   time.Time `json:"createdAt"`
}

type selfStatus struct {
	Module                  string         `json:"module"`
	Status                  string         `json:"status"`
	UnknownAttempts         int64          `json:"unknownAttempts"`
	UnresolvedAttempts      int64          `json:"unresolvedAttempts"`
	ExpiredRunningAttempts  int64          `json:"expiredRunningAttempts"`
	ActiveDebtCount         int64          `json:"activeDebtCount"`
	ActiveDebtObjects       int64          `json:"activeDebtObjects"`
	VerifiedEvidenceBacklog int64          `json:"verifiedEvidenceBacklog"`
	SecurityIncidentCount   int64          `json:"securityIncidentCount"`
	ActionRequired          bool           `json:"actionRequired"`
	Incidents               []selfIncident `json:"incidents"`
	Partial                 bool           `json:"partial"`
}

func (s *Store) SystemStatus(ctx context.Context, identity orchestration.Identity) ([]byte, error) {
	if identity.TenantID == "" {
		return nil, fmt.Errorf("tenant identity is required for MCP self-status")
	}
	var unknown, unresolved, expired int64
	if err := s.pool.QueryRow(ctx, `
		select
			count(*) filter(where state='UNKNOWN'),
			count(*) filter(where state='UNRESOLVED'),
			count(*) filter(where state='RUNNING' and lease_until < transaction_timestamp())
		from ouf_mcp.tool_attempt where tenant_id=$1`, identity.TenantID).Scan(&unknown, &unresolved, &expired); err != nil {
		return nil, err
	}
	var debtCount, debtObjects int64
	if err := s.pool.QueryRow(ctx, `
		select count(*),coalesce(sum(d.debt_amount),0)
		from ouf_mcp.budget_object_debt d
		join ouf_mcp.tool_attempt a on a.attempt_id=d.attempt_id
		where a.tenant_id=$1 and d.debt_state='ACTIVE'`, identity.TenantID).Scan(&debtCount, &debtObjects); err != nil {
		return nil, err
	}
	var evidenceBacklog int64
	if err := s.pool.QueryRow(ctx, `
		select count(*)
		from ouf_mcp.owner_evidence_inbox e
		join ouf_mcp.tool_attempt a on a.attempt_id=e.attempt_id
		where a.tenant_id=$1 and e.evidence_state='VERIFIED'`, identity.TenantID).Scan(&evidenceBacklog); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
		select i.incident_key,i.category,i.detail_code,i.created_at
		from ouf_mcp.security_incident i
		join ouf_mcp.tool_attempt a on a.attempt_id=i.attempt_id
		where a.tenant_id=$1
		order by i.created_at desc
		limit 20`, identity.TenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	incidents := make([]selfIncident, 0, 20)
	for rows.Next() {
		var item selfIncident
		if err := rows.Scan(&item.IncidentKey, &item.Category, &item.DetailCode, &item.CreatedAt); err != nil {
			return nil, err
		}
		incidents = append(incidents, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	status := "HEALTHY"
	if unknown > 0 || expired > 0 || debtCount > 0 || evidenceBacklog > 0 {
		status = "RECOVERING"
	}
	if unresolved > 0 || len(incidents) > 0 {
		status = "DEGRADED"
	}
	out := selfStatus{
		Module: "MCP", Status: status,
		UnknownAttempts: unknown, UnresolvedAttempts: unresolved, ExpiredRunningAttempts: expired,
		ActiveDebtCount: debtCount, ActiveDebtObjects: debtObjects, VerifiedEvidenceBacklog: evidenceBacklog,
		SecurityIncidentCount: int64(len(incidents)), ActionRequired: unresolved > 0 || len(incidents) > 0,
		Incidents: incidents, Partial: false,
	}
	return json.Marshal(out)
}
