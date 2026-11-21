create table ouf_mcp.unit_profile_origin (
 service_principal_id text not null,
 principal_id text not null,
 tenant_id text not null,
 profile_id uuid not null,
 asset_id uuid not null,
 primary key (service_principal_id, principal_id, tenant_id, profile_id)
);
