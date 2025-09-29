"""Internal JWT token issuer with refresh-token rotation."""

from __future__ import annotations

import base64
import hashlib
import json
import secrets
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Sequence, Set, Tuple

from jose import jwt

try:  # pragma: no cover - optional dependency guard
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
except Exception as exc:  # pragma: no cover - ensure useful error later
    raise RuntimeError("cryptography is required for token_service") from exc

try:  # pragma: no cover - optional dependency guard
    import redis
except Exception:  # pragma: no cover - redis optional
    redis = None  # type: ignore[assignment]


class TokenServiceError(Exception):
    """Base error for token service operations."""


class InvalidRefreshTokenError(TokenServiceError):
    """Raised when refresh tokens are invalid or expired."""


@dataclass(frozen=True)
class KeyPair:
    """Represents a signing key pair with metadata."""

    kid: str
    private_pem: bytes
    public_pem: bytes
    algorithm: str

    @property
    def public_jwk(self) -> Dict[str, str]:
        public_key = serialization.load_pem_public_key(self.public_pem)
        if not hasattr(public_key, "public_numbers"):
            raise ValueError("Unsupported key type for JWK export")
        numbers = public_key.public_numbers()
        return {
            "kty": "RSA",
            "kid": self.kid,
            "alg": self.algorithm,
            "use": "sig",
            "n": _b64url_uint(numbers.n),
            "e": _b64url_uint(numbers.e),
        }


@dataclass(frozen=True)
class TokenServiceConfig:
    """Configuration for token issuance."""

    issuer: str
    audiences: Tuple[str, ...]
    access_token_ttl: int
    refresh_token_ttl: int
    algorithm: str = "RS256"


@dataclass
class RefreshTokenRecord:
    """Stored refresh token metadata."""

    token_hash: str
    subject: str
    tenant_id: Optional[str]
    scopes: Tuple[str, ...]
    claims: Dict[str, Any]
    session_id: str
    expires_at: datetime

    def is_expired(self, now: datetime) -> bool:
        return now >= self.expires_at


class RefreshTokenStore:
    """Abstraction for refresh token persistence."""

    def store(self, record: RefreshTokenRecord) -> None:
        raise NotImplementedError

    def pop(self, token_hash: str) -> Optional[RefreshTokenRecord]:
        raise NotImplementedError

    def get(self, token_hash: str) -> Optional[RefreshTokenRecord]:
        raise NotImplementedError


class InMemoryRefreshTokenStore(RefreshTokenStore):
    """Thread-safe in-memory refresh token store."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._records: Dict[str, RefreshTokenRecord] = {}

    def store(self, record: RefreshTokenRecord) -> None:
        with self._lock:
            self._records[record.token_hash] = record

    def pop(self, token_hash: str) -> Optional[RefreshTokenRecord]:
        with self._lock:
            return self._records.pop(token_hash, None)

    def get(self, token_hash: str) -> Optional[RefreshTokenRecord]:
        with self._lock:
            return self._records.get(token_hash)

    def purge_expired(self) -> None:
        now = datetime.now(timezone.utc)
        with self._lock:
            expired = [key for key, record in self._records.items() if record.is_expired(now)]
            for key in expired:
                self._records.pop(key, None)


class RedisRefreshTokenStore(RefreshTokenStore):
    """Redis-backed refresh token persistence."""

    def __init__(
        self,
        client: "redis.Redis",
        *,
        namespace: str = "aurum:auth:refresh_tokens",
    ) -> None:
        if redis is None:
            raise RuntimeError("redis-py is required for RedisRefreshTokenStore")
        self._client = client
        self._namespace = namespace.rstrip(":")

    def _key(self, token_hash: str) -> str:
        return f"{self._namespace}:{token_hash}"

    def store(self, record: RefreshTokenRecord) -> None:
        ttl_seconds = int((record.expires_at - datetime.now(timezone.utc)).total_seconds())
        ttl_seconds = max(ttl_seconds, 1)
        payload = {
            "subject": record.subject,
            "tenant_id": record.tenant_id,
            "scopes": list(record.scopes),
            "claims": record.claims,
            "session_id": record.session_id,
            "expires_at": record.expires_at.isoformat(),
        }
        self._client.set(self._key(record.token_hash), json.dumps(payload), ex=ttl_seconds)

    def pop(self, token_hash: str) -> Optional[RefreshTokenRecord]:
        key = self._key(token_hash)
        with self._client.pipeline() as pipe:
            pipe.get(key)
            pipe.delete(key)
            data, _ = pipe.execute()
        if not data:
            return None
        return self._decode_record(token_hash, data)

    def get(self, token_hash: str) -> Optional[RefreshTokenRecord]:
        data = self._client.get(self._key(token_hash))
        if not data:
            return None
        return self._decode_record(token_hash, data)

    def _decode_record(self, token_hash: str, raw: Any) -> Optional[RefreshTokenRecord]:
        try:
            payload = json.loads(raw)
            expires_at = datetime.fromisoformat(payload["expires_at"])
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)
            return RefreshTokenRecord(
                token_hash=token_hash,
                subject=payload["subject"],
                tenant_id=payload.get("tenant_id"),
                scopes=tuple(payload.get("scopes", [])),
                claims=payload.get("claims", {}),
                session_id=payload["session_id"],
                expires_at=expires_at,
            )
        except Exception:
            return None


class TokenService:
    """Issues signed access tokens and manages refresh token lifecycle."""

    def __init__(
        self,
        config: TokenServiceConfig,
        store: Optional[RefreshTokenStore] = None,
    ) -> None:
        self.config = config
        self.store = store or InMemoryRefreshTokenStore()
        self._lock = threading.Lock()
        self._current_key = self._generate_key_pair()
        self._revoked_sessions: Set[str] = set()

    def issue_token_pair(
        self,
        *,
        subject: str,
        tenant_id: Optional[str],
        scopes: Sequence[str] = (),
        base_claims: Optional[Dict[str, Any]] = None,
        session_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        session = session_id or str(uuid.uuid4())
        access_expires = now + timedelta(seconds=self.config.access_token_ttl)
        refresh_expires = now + timedelta(seconds=self.config.refresh_token_ttl)

        claims = {
            "iss": self.config.issuer,
            "sub": subject,
            "aud": list(self.config.audiences) or None,
            "jti": str(uuid.uuid4()),
            "iat": int(now.timestamp()),
            "nbf": int(now.timestamp()),
            "exp": int(access_expires.timestamp()),
            "tenant": tenant_id,
            "scope": " ".join(sorted(set(scopes))),
            "sid": session,
        }
        if tenant_id is not None:
            claims["tenant_id"] = tenant_id
        if base_claims:
            for key, value in base_claims.items():
                if key in {"iss", "aud", "exp", "nbf", "iat", "jti", "sid"}:
                    continue
                claims[key] = value

        headers = {"kid": self._current_key.kid, "alg": self._current_key.algorithm}
        token = jwt.encode(claims, self._current_key.private_pem, algorithm=self._current_key.algorithm, headers=headers)

        refresh_token = secrets.token_urlsafe(64)
        refresh_record = RefreshTokenRecord(
            token_hash=_hash_token(refresh_token),
            subject=subject,
            tenant_id=tenant_id,
            scopes=tuple(scopes),
            claims=dict(base_claims or {}),
            session_id=session,
            expires_at=refresh_expires,
        )
        self.store.store(refresh_record)

        return {
            "access_token": token,
            "refresh_token": refresh_token,
            "expires_in": self.config.access_token_ttl,
            "refresh_expires_in": self.config.refresh_token_ttl,
            "token_type": "Bearer",
            "scope": " ".join(sorted(set(scopes))),
            "session_id": session,
            "tenant": tenant_id,
        }

    def refresh(self, refresh_token: str) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        record = self.store.pop(_hash_token(refresh_token))
        if record is None or record.is_expired(now) or self.is_session_revoked(record.session_id):
            raise InvalidRefreshTokenError("invalid_refresh_token")
        return self.issue_token_pair(
            subject=record.subject,
            tenant_id=record.tenant_id,
            scopes=record.scopes,
            base_claims=record.claims,
            session_id=record.session_id,
        )

    def revoke_refresh_token(self, refresh_token: str) -> None:
        record = self.store.pop(_hash_token(refresh_token))
        if record:
            with self._lock:
                self._revoked_sessions.add(record.session_id)

    def revoke_session(self, session_id: str) -> None:
        with self._lock:
            self._revoked_sessions.add(session_id)

    def is_session_revoked(self, session_id: Optional[str]) -> bool:
        if not session_id:
            return False
        with self._lock:
            return session_id in self._revoked_sessions

    def jwks(self) -> Dict[str, Any]:
        return {"keys": [self._current_key.public_jwk]}

    def rotate_keys(self) -> None:
        with self._lock:
            self._current_key = self._generate_key_pair()

    def _generate_key_pair(self) -> KeyPair:
        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        private_pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        public_pem = private_key.public_key().public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        kid = base64.urlsafe_b64encode(uuid.uuid4().bytes).rstrip(b"=").decode("ascii")
        return KeyPair(kid=kid, private_pem=private_pem, public_pem=public_pem, algorithm=self.config.algorithm)


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _b64url_uint(value: int) -> str:
    byte_length = (value.bit_length() + 7) // 8
    return base64.urlsafe_b64encode(value.to_bytes(byte_length, "big")).rstrip(b"=").decode("ascii")


__all__ = [
    "TokenServiceConfig",
    "TokenService",
    "TokenServiceError",
    "InvalidRefreshTokenError",
    "RefreshTokenRecord",
    "RefreshTokenStore",
    "InMemoryRefreshTokenStore",
    "RedisRefreshTokenStore",
]
