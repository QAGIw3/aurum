# Comprehensive Vault policy for secrets lifecycle management
# This policy provides capabilities for auto-rotation, key escrow, and access auditing

# Secrets management paths
path "secret/data/aurum/*" {
  capabilities = ["create", "read", "update", "delete", "list"]
  description = "Full access to Aurum application secrets"
}

path "secret/metadata/aurum/*" {
  capabilities = ["create", "read", "update", "delete", "list"]
  description = "Access to secret metadata for lifecycle management"
}

# Database secrets engine
path "database/*" {
  capabilities = ["create", "read", "update", "delete", "list"]
  description = "Manage database credentials"
}

# Transit secrets engine for encryption
path "transit/*" {
  capabilities = ["create", "read", "update", "delete", "list"]
  description = "Manage encryption keys and operations"
}

# PKI secrets engine for certificates
path "pki/*" {
  capabilities = ["create", "read", "update", "delete", "list"]
  description = "Manage PKI certificates and keys"
}

# AWS secrets engine
path "aws/*" {
  capabilities = ["create", "read", "update", "delete", "list"]
  description = "Manage AWS credentials"
}

# Key management for escrow
path "transit/keys/escrow" {
  capabilities = ["create", "read", "update"]
  description = "Key escrow operations"
}

path "transit/encrypt/escrow" {
  capabilities = ["create", "update"]
  description = "Encrypt data with escrow key"
}

path "transit/decrypt/escrow" {
  capabilities = ["create", "update"]
  description = "Decrypt data with escrow key"
}

# Backup and restore operations
path "sys/storage/raft/snapshot" {
  capabilities = ["read"]
  description = "Take Vault snapshots for backup"
}

path "sys/storage/raft/snapshot-force" {
  capabilities = ["create"]
  description = "Force snapshot creation"
}

# Audit logging access
path "sys/audit" {
  capabilities = ["read", "sudo"]
  description = "Access audit logs"
}

path "sys/audit-hash/*" {
  capabilities = ["read"]
  description = "Read audit hash files"
}

# Token management for lifecycle
path "auth/token/create" {
  capabilities = ["create", "update", "sudo"]
  description = "Create tokens for service accounts"
}

path "auth/token/create-orphan" {
  capabilities = ["create", "update", "sudo"]
  description = "Create orphan tokens"
}

path "auth/token/revoke-orphan" {
  capabilities = ["update", "sudo"]
  description = "Revoke orphan tokens"
}

# Policy management
path "sys/policies/acl" {
  capabilities = ["create", "read", "update", "delete", "list", "sudo"]
  description = "Manage ACL policies"
}

path "sys/policies/acl/*" {
  capabilities = ["create", "read", "update", "delete", "list", "sudo"]
  description = "Manage specific ACL policies"
}

# Health and status
path "sys/health" {
  capabilities = ["read"]
  description = "Check Vault health"
}

path "sys/seal-status" {
  capabilities = ["read"]
  description = "Check seal status"
}

# Key rotation and rekeying
path "sys/rekey" {
  capabilities = ["create", "update", "read", "delete", "sudo"]
  description = "Rekey operations"
}

path "sys/rekey/*" {
  capabilities = ["create", "update", "read", "delete", "sudo"]
  description = "Detailed rekey operations"
}

# Rotate specific paths
path "secret/rotate/aurum/*" {
  capabilities = ["create", "update", "read", "sudo"]
  description = "Rotate specific secrets"
}

# Key escrow operations
path "sys/key-status" {
  capabilities = ["read"]
  description = "Check key status for escrow"
}

# Deny access to sensitive system paths
path "sys/mounts" {
  capabilities = ["deny"]
  description = "Deny mount configuration access"
}

path "sys/mounts/*" {
  capabilities = ["deny"]
  description = "Deny mount configuration access"
}

path "sys/auth" {
  capabilities = ["deny"]
  description = "Deny auth backend configuration"
}

path "sys/auth/*" {
  capabilities = ["deny"]
  description = "Deny auth backend configuration"
}

# Allow lease management
path "sys/leases" {
  capabilities = ["read", "list"]
  description = "List leases"
}

path "sys/leases/renew" {
  capabilities = ["create", "update"]
  description = "Renew leases"
}

path "sys/leases/revoke" {
  capabilities = ["create", "update"]
  description = "Revoke leases"
}

# Allow namespace operations
path "sys/namespaces" {
  capabilities = ["create", "read", "update", "delete", "list"]
  description = "Manage namespaces"
}

path "sys/namespaces/*" {
  capabilities = ["create", "read", "update", "delete", "list"]
  description = "Manage specific namespaces"
}
