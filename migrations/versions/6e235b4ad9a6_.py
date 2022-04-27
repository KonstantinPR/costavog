"""empty message

Revision ID: 6e235b4ad9a6
Revises: 4f9f649efbb5
Create Date: 2022-04-27 20:50:35.307385

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '6e235b4ad9a6'
down_revision = '4f9f649efbb5'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index('ix_temp_table_index', table_name='temp_table')
    op.drop_table('temp_table')
    op.alter_column('products', 'article',
               existing_type=sa.TEXT(),
               nullable=False)
    op.create_unique_constraint(None, 'products', ['article'])
    op.create_foreign_key(None, 'products', 'companies', ['company_id'], ['id'])
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'products', type_='foreignkey')
    op.drop_constraint(None, 'products', type_='unique')
    op.alter_column('products', 'article',
               existing_type=sa.TEXT(),
               nullable=True)
    op.create_table('temp_table',
    sa.Column('index', sa.BIGINT(), autoincrement=False, nullable=True),
    sa.Column('company_id', sa.BIGINT(), autoincrement=False, nullable=True),
    sa.Column('article', sa.TEXT(), autoincrement=False, nullable=True),
    sa.Column('net_cost', sa.BIGINT(), autoincrement=False, nullable=True)
    )
    op.create_index('ix_temp_table_index', 'temp_table', ['index'], unique=False)
    # ### end Alembic commands ###