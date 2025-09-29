"""Configuration tracking tables migration.

Revision ID: 20250102_01_config_tracking_tables
Revises: None
Create Date: 2025-01-02 00:00:00.000000

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '20250102_01_config_tracking_tables'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    """Create configuration tracking tables."""

    # Configuration versions table
    op.create_table(
        'config_versions',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('version', sa.Integer(), nullable=False, unique=True),
        sa.Column('timestamp', sa.DateTime(timezone=True), nullable=False),
        sa.Column('content_hash', sa.String(64), nullable=False),
        sa.Column('config_data', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('change_id', sa.String(36), nullable=False),
        sa.Column('compressed_size', sa.Integer(), nullable=False, default=0),
        sa.Column('metadata', postgresql.JSONB(astext_type=sa.Text()), nullable=False, default={}),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.Index('ix_config_versions_version', 'version'),
        sa.Index('ix_config_versions_timestamp', 'timestamp'),
        sa.Index('ix_config_versions_content_hash', 'content_hash'),
        sa.Index('ix_config_versions_change_id', 'change_id')
    )

    # Configuration changes table
    op.create_table(
        'config_changes',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('change_id', sa.String(36), nullable=False, unique=True),
        sa.Column('timestamp', sa.DateTime(timezone=True), nullable=False),
        sa.Column('change_type', sa.String(20), nullable=False),
        sa.Column('source', sa.String(20), nullable=False),
        sa.Column('actor', sa.String(255), nullable=False),
        sa.Column('namespace', sa.String(100), nullable=True),
        sa.Column('reason', sa.Text(), nullable=True),
        sa.Column('correlation_id', sa.String(36), nullable=True),
        sa.Column('old_config', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('new_config', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('diff_data', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('metadata', postgresql.JSONB(astext_type=sa.Text()), nullable=False, default={}),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.Index('ix_config_changes_change_id', 'change_id'),
        sa.Index('ix_config_changes_timestamp', 'timestamp'),
        sa.Index('ix_config_changes_actor', 'actor'),
        sa.Index('ix_config_changes_namespace', 'namespace'),
        sa.Index('ix_config_changes_correlation_id', 'correlation_id')
    )

    # Configuration events table (for Kafka-like event storage)
    op.create_table(
        'config_events',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('event_id', sa.String(36), nullable=False, unique=True),
        sa.Column('event_type', sa.String(50), nullable=False),
        sa.Column('timestamp', sa.DateTime(timezone=True), nullable=False),
        sa.Column('change_id', sa.String(36), nullable=True),
        sa.Column('event_data', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('processed', sa.Boolean(), nullable=False, default=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.Index('ix_config_events_event_id', 'event_id'),
        sa.Index('ix_config_events_timestamp', 'timestamp'),
        sa.Index('ix_config_events_change_id', 'change_id'),
        sa.Index('ix_config_events_event_type', 'event_type')
    )

    # Configuration backups table
    op.create_table(
        'config_backups',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('backup_id', sa.String(36), nullable=False, unique=True),
        sa.Column('version', sa.Integer(), nullable=False),
        sa.Column('timestamp', sa.DateTime(timezone=True), nullable=False),
        sa.Column('backup_path', sa.String(500), nullable=False),
        sa.Column('reason', sa.Text(), nullable=True),
        sa.Column('size_bytes', sa.BigInteger(), nullable=False),
        sa.Column('metadata', postgresql.JSONB(astext_type=sa.Text()), nullable=False, default={}),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.Index('ix_config_backups_backup_id', 'backup_id'),
        sa.Index('ix_config_backups_version', 'version'),
        sa.Index('ix_config_backups_timestamp', 'timestamp')
    )


def downgrade():
    """Drop configuration tracking tables."""

    op.drop_table('config_backups')
    op.drop_table('config_events')
    op.drop_table('config_changes')
    op.drop_table('config_versions')
