"""empty message

Revision ID: 8aab62b445c4
Revises: 150c35ad470b
Create Date: 2022-06-02 16:29:46.321983

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '8aab62b445c4'
down_revision = '150c35ad470b'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('companies',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('company_name', sa.String(length=80), nullable=True),
    sa.Column('password_hash', sa.String(length=500), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('tasks',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('amount', sa.String(length=80), nullable=True),
    sa.Column('description', sa.String(length=100), nullable=True),
    sa.Column('user_name', sa.String(length=100), nullable=True),
    sa.Column('company_id', sa.Integer(), nullable=True),
    sa.Column('date', sa.String(length=100), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('transactions',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('amount', sa.String(length=80), nullable=True),
    sa.Column('description', sa.String(length=500), nullable=True),
    sa.Column('user_name', sa.String(length=100), nullable=True),
    sa.Column('company_id', sa.Integer(), nullable=True),
    sa.Column('date', sa.String(length=100), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('products',
    sa.Column('article', sa.String(length=80), nullable=False),
    sa.Column('net_cost', sa.Integer(), nullable=True),
    sa.Column('company_id', sa.Integer(), nullable=True),
    sa.ForeignKeyConstraint(['company_id'], ['companies.id'], ),
    sa.PrimaryKeyConstraint('article')
    )
    op.create_table('users',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('user_name', sa.String(length=100), nullable=True),
    sa.Column('password_hash', sa.String(length=500), nullable=True),
    sa.Column('company_id', sa.Integer(), nullable=True),
    sa.Column('initial_sum', sa.Integer(), nullable=True),
    sa.Column('initial_file_path', sa.String(length=500), nullable=True),
    sa.Column('yandex_disk_token', sa.String(length=1000), nullable=True),
    sa.Column('role', sa.String(length=500), nullable=True),
    sa.ForeignKeyConstraint(['company_id'], ['companies.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('users')
    op.drop_table('products')
    op.drop_table('transactions')
    op.drop_table('tasks')
    op.drop_table('companies')
    # ### end Alembic commands ###