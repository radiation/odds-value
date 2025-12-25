"""add week metadata to games

Revision ID: ee1d7921d305
Revises: f04ba40e4215
Create Date: 2025-12-25 13:40:14.842277

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'ee1d7921d305'
down_revision: Union[str, Sequence[str], None] = 'f04ba40e4215'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
