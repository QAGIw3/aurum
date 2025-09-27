# Vault policy for external data access
# This policy provides least-privilege access for external data ingestion

path "secret/data/external/*" {
  capabilities = ["read", "list"]
  description = "Read external provider credentials and API keys"
}

path "secret/data/external/fred/*" {
  capabilities = ["read"]
  description = "Access FRED API credentials"
}

path "secret/data/external/eia/*" {
  capabilities = ["read"]
  description = "Access EIA API credentials"
}

path "secret/data/external/bls/*" {
  capabilities = ["read"]
  description = "Access BLS API credentials"
}

path "secret/data/external/oecd/*" {
  capabilities = ["read"]
  description = "Access OECD API credentials"
}

path "secret/data/aws/*" {
  capabilities = ["read"]
  description = "Read AWS credentials for S3 access"
}

path "secret/data/gcp/*" {
  capabilities = ["read"]
  description = "Read GCP credentials for Cloud Storage access"
}

path "secret/data/azure/*" {
  capabilities = ["read"]
  description = "Read Azure credentials for Blob Storage access"
}

path "secret/data/database/*" {
  capabilities = ["read"]
  description = "Read database connection credentials"
}

# Deny write access to prevent credential updates from pods
path "secret/data/external/*" {
  capabilities = ["deny"]
  description = "Explicitly deny write access to prevent credential updates"
}

# Allow reading audit logs (read-only)
path "sys/audit" {
  capabilities = ["read"]
  description = "Read audit logs for compliance"
}

path "sys/audit-hash/*" {
  capabilities = ["read"]
  description = "Read audit hash for verification"
}

# Deny access to other sensitive paths
path "sys/*" {
  capabilities = ["deny"]
  description = "Deny access to system configuration"
}

path "auth/*" {
  capabilities = ["deny"]
  description = "Deny access to authentication backends"
}

path "identity/*" {
  capabilities = ["deny"]
  description = "Deny access to identity management"
}

# Allow token renewal for the service
path "auth/token/renew-self" {
  capabilities = ["update"]
  description = "Allow token renewal"
}

path "auth/token/lookup-self" {
  capabilities = ["read"]
  description = "Allow token introspection"
}
