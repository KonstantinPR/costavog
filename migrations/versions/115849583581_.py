"""empty message

Revision ID: 115849583581
Revises: f60e6f3f1954
Create Date: 2022-07-22 15:53:40.740427

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '115849583581'
down_revision = 'f60e6f3f1954'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index('ix_temp_table_index', table_name='temp_table')
    op.drop_table('temp_table')
    op.add_column('companies', sa.Column('wb_api_token2', sa.String(length=1000), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('companies', 'wb_api_token2')
    op.create_table('temp_table',
    sa.Column('index', sa.BIGINT(), autoincrement=False, nullable=True),
    sa.Column('article', sa.TEXT(), autoincrement=False, nullable=True),
    sa.Column('net_cost', sa.BIGINT(), autoincrement=False, nullable=True),
    sa.Column('company_id', sa.BIGINT(), autoincrement=False, nullable=True)
    )
    op.create_index('ix_temp_table_index', 'temp_table', ['index'], unique=False)
    # ### end Alembic commands ###