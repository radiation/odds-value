"""add off/def windows to team_game_state

Revision ID: 9955d64fa0fb
Revises: 856836e32e99
Create Date: 2026-01-03 08:08:21.811430

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "9955d64fa0fb"
down_revision: Union[str, Sequence[str], None] = "856836e32e99"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
