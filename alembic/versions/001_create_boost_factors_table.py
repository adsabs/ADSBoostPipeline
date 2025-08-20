"""Create boost_factors table

Revision ID: 001
Revises: 
Create Date: 2024-01-01 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '001'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    """Create boost_factors table with simplified boost algorithm structure"""
    op.create_table('boost_factors',
        sa.Column('id', sa.Integer(), nullable=False, index=True),
        sa.Column('bibcode', sa.String(length=19), nullable=True, index=True),
        sa.Column('scix_id', sa.String(length=19), nullable=True, index=True),
        sa.Column('created', sa.DateTime(), nullable=True),
        
        # Basic boost factors
        sa.Column('refereed_boost', sa.Float(), nullable=True),
        sa.Column('doctype_boost', sa.Float(), nullable=True),
        sa.Column('recency_boost', sa.Float(), nullable=True),
        
        # Combined boost factor (weighted average of basic boosts)
        sa.Column('boost_factor', sa.Float(), nullable=True),
        
        # Collection weights
        sa.Column('astronomy_weight', sa.Float(), nullable=True),
        sa.Column('physics_weight', sa.Float(), nullable=True),
        sa.Column('earth_science_weight', sa.Float(), nullable=True),
        sa.Column('planetary_science_weight', sa.Float(), nullable=True),
        sa.Column('heliophysics_weight', sa.Float(), nullable=True),
        sa.Column('general_weight', sa.Float(), nullable=True),
        
        # Discipline-specific final boosts (discipline_weight * boost_factor)
        sa.Column('astronomy_final_boost', sa.Float(), nullable=True),
        sa.Column('physics_final_boost', sa.Float(), nullable=True),
        sa.Column('earth_science_final_boost', sa.Float(), nullable=True),
        sa.Column('planetary_science_final_boost', sa.Float(), nullable=True),
        sa.Column('heliophysics_final_boost', sa.Float(), nullable=True),
        sa.Column('general_final_boost', sa.Float(), nullable=True),
        
        sa.PrimaryKeyConstraint('id')
    )
    

def downgrade():
    """Drop boost_factors table"""
    op.drop_table('boost_factors') 