"""empty message

Revision ID: f60e6f3f1954
Revises: 477046d176ff
Create Date: 2022-06-22 14:14:37.946274

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'f60e6f3f1954'
down_revision = '477046d176ff'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('companies', sa.Column('yandex_disk_token', sa.String(length=1000), nullable=True))
    op.add_column('companies', sa.Column('wb_api_token', sa.String(length=1000), nullable=True))
    op.drop_column('users', 'yandex_disk_token')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('users', sa.Column('yandex_disk_token', sa.VARCHAR(length=1000), autoincrement=False, nullable=True))
    op.drop_column('companies', 'wb_api_token')
    op.drop_column('companies', 'yandex_disk_token')
    # ### end Alembic commands ###