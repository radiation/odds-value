"""add off/def windows to team_game_state

Revision ID: 7cc8e7009fff
Revises: e767c858f106
Create Date: 2026-01-03 07:20:54.511023

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "7cc8e7009fff"
down_revision: Union[str, Sequence[str], None] = "e767c858f106"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
