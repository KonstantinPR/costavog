"""empty message

Revision ID: 412b1eab031d
Revises: 81a8fdb41d22
Create Date: 2022-09-05 15:15:25.438035

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '412b1eab031d'
down_revision = '81a8fdb41d22'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('transactions', 'is_private',
               existing_type=sa.INTEGER(),
               nullable=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('transactions', 'is_private',
               existing_type=sa.INTEGER(),
               nullable=True)
    # ### end Alembic commands ###
