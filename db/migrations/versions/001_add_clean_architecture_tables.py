"""Add clean architecture tables for domain models

Revision ID: 001_clean_arch
Revises: 
Create Date: 2025-10-02

This migration adds tables for the new clean architecture implementation:
- Curves and curve points
- ISO markets and market data (LMP, Load, Generation Mix)
- PPAs and delivery schedules
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '001_clean_arch'
down_revision = None  # Update this to point to your latest migration
branch_labels = None
depends_on = None


def upgrade():
    """Create new clean architecture tables."""
    
    # Create curves table
    op.create_table(
        'curves',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('tenant_id', postgresql.UUID(as_uuid=True), nullable=False, index=True),
        sa.Column('curve_key', sa.String(255), nullable=False, index=True),
        sa.Column('as_of_date', sa.DateTime(), nullable=False, index=True),
        sa.Column('currency', sa.String(3), nullable=False, default='USD'),
        sa.Column('tenor_type', sa.String(50)),
        sa.Column('price_type', sa.String(50)),
        sa.Column('day_count', sa.String(50)),
        sa.Column('calendar', sa.String(50)),
        sa.Column('asset_class', sa.String(50)),
        sa.Column('source', sa.String(100)),
        sa.Column('measure', sa.String(50), nullable=False, default='value'),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('version', sa.Integer(), nullable=False, default=0),
    )
    
    # Create curve_points table
    op.create_table(
        'curve_points',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('curve_id', postgresql.UUID(as_uuid=True), nullable=False, index=True),
        sa.Column('tenor', sa.Numeric(precision=20, scale=6), nullable=False),
        sa.Column('value', sa.Numeric(precision=20, scale=6), nullable=False),
        sa.Column('timestamp', sa.DateTime()),
        sa.Column('quality_flag', sa.String(50)),
        sa.ForeignKeyConstraint(['curve_id'], ['curves.id'], ondelete='CASCADE'),
    )
    
    # Create iso_markets table
    op.create_table(
        'iso_markets',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('tenant_id', postgresql.UUID(as_uuid=True), nullable=False, index=True),
        sa.Column('iso_code', sa.String(10), nullable=False, unique=True, index=True),
        sa.Column('iso_name', sa.String(255), nullable=False),
        sa.Column('timezone', sa.String(50), nullable=False),
        sa.Column('active', sa.Boolean(), nullable=False, default=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('version', sa.Integer(), nullable=False, default=0),
    )
    
    # Create lmp_data table
    op.create_table(
        'lmp_data',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('iso_market_id', postgresql.UUID(as_uuid=True), nullable=False, index=True),
        sa.Column('node_id', sa.String(100), nullable=False, index=True),
        sa.Column('location_zone', sa.String(100)),
        sa.Column('location_node', sa.String(100)),
        sa.Column('energy_price', sa.Numeric(precision=20, scale=6), nullable=False),
        sa.Column('congestion_price', sa.Numeric(precision=20, scale=6), nullable=False),
        sa.Column('loss_price', sa.Numeric(precision=20, scale=6), nullable=False),
        sa.Column('timestamp', sa.DateTime(), nullable=False, index=True),
        sa.Column('market_type', sa.String(10), nullable=False),
        sa.ForeignKeyConstraint(['iso_market_id'], ['iso_markets.id'], ondelete='CASCADE'),
    )
    
    # Create load_data table
    op.create_table(
        'load_data',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('iso_market_id', postgresql.UUID(as_uuid=True), nullable=False, index=True),
        sa.Column('zone_id', sa.String(100), nullable=False, index=True),
        sa.Column('load_mw', sa.Numeric(precision=20, scale=6), nullable=False),
        sa.Column('timestamp', sa.DateTime(), nullable=False, index=True),
        sa.Column('forecast', sa.Boolean(), nullable=False, default=False),
        sa.ForeignKeyConstraint(['iso_market_id'], ['iso_markets.id'], ondelete='CASCADE'),
    )
    
    # Create generation_mix_data table
    op.create_table(
        'generation_mix_data',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('iso_market_id', postgresql.UUID(as_uuid=True), nullable=False, index=True),
        sa.Column('zone_id', sa.String(100), nullable=False, index=True),
        sa.Column('fuel_type', sa.String(50), nullable=False, index=True),
        sa.Column('generation_mw', sa.Numeric(precision=20, scale=6), nullable=False),
        sa.Column('percentage', sa.Numeric(precision=5, scale=2), nullable=False),
        sa.Column('timestamp', sa.DateTime(), nullable=False, index=True),
        sa.ForeignKeyConstraint(['iso_market_id'], ['iso_markets.id'], ondelete='CASCADE'),
    )
    
    # Create ppas table
    op.create_table(
        'ppas',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('tenant_id', postgresql.UUID(as_uuid=True), nullable=False, index=True),
        sa.Column('contract_number', sa.String(100), nullable=False, unique=True, index=True),
        sa.Column('buyer_name', sa.String(255), nullable=False),
        sa.Column('seller_name', sa.String(255), nullable=False),
        sa.Column('status', sa.String(50), nullable=False, index=True),
        sa.Column('pricing_type', sa.String(50), nullable=False),
        sa.Column('fixed_price_amount', sa.Numeric(precision=20, scale=6)),
        sa.Column('fixed_price_currency', sa.String(3)),
        sa.Column('floor_price_amount', sa.Numeric(precision=20, scale=6)),
        sa.Column('floor_price_currency', sa.String(3)),
        sa.Column('ceiling_price_amount', sa.Numeric(precision=20, scale=6)),
        sa.Column('ceiling_price_currency', sa.String(3)),
        sa.Column('index_reference', sa.String(100)),
        sa.Column('index_multiplier', sa.Numeric(precision=10, scale=4), default=1.0),
        sa.Column('delivery_start', sa.DateTime()),
        sa.Column('delivery_end', sa.DateTime()),
        sa.Column('minimum_annual_mwh', sa.Numeric(precision=20, scale=6)),
        sa.Column('maximum_annual_mwh', sa.Numeric(precision=20, scale=6)),
        sa.Column('signed_date', sa.DateTime()),
        sa.Column('effective_date', sa.DateTime()),
        sa.Column('expiration_date', sa.DateTime()),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('version', sa.Integer(), nullable=False, default=0),
    )
    
    # Create delivery_schedules table
    op.create_table(
        'delivery_schedules',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('ppa_id', postgresql.UUID(as_uuid=True), nullable=False, index=True),
        sa.Column('schedule_id', sa.String(100), nullable=False, unique=True, index=True),
        sa.Column('delivery_date', sa.DateTime(), nullable=False, index=True),
        sa.Column('scheduled_mwh', sa.Numeric(precision=20, scale=6), nullable=False),
        sa.Column('actual_mwh', sa.Numeric(precision=20, scale=6)),
        sa.Column('delivery_location', sa.String(255)),
        sa.ForeignKeyConstraint(['ppa_id'], ['ppas.id'], ondelete='CASCADE'),
    )
    
    # Create composite indexes for common queries
    op.create_index('ix_curves_tenant_key_date', 'curves', ['tenant_id', 'curve_key', 'as_of_date'])
    op.create_index('ix_lmp_data_iso_node_time', 'lmp_data', ['iso_market_id', 'node_id', 'timestamp'])
    op.create_index('ix_load_data_iso_zone_time', 'load_data', ['iso_market_id', 'zone_id', 'timestamp'])
    op.create_index('ix_gen_mix_iso_zone_time', 'generation_mix_data', ['iso_market_id', 'zone_id', 'timestamp'])
    op.create_index('ix_delivery_schedules_ppa_date', 'delivery_schedules', ['ppa_id', 'delivery_date'])


def downgrade():
    """Drop clean architecture tables."""
    
    # Drop tables in reverse order (child tables first)
    op.drop_table('delivery_schedules')
    op.drop_table('ppas')
    op.drop_table('generation_mix_data')
    op.drop_table('load_data')
    op.drop_table('lmp_data')
    op.drop_table('iso_markets')
    op.drop_table('curve_points')
    op.drop_table('curves')

