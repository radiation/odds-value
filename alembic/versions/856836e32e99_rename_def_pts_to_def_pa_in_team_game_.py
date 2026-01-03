"""rename def_pts to def_pa in team_game_state

Revision ID: 856836e32e99
Revises: 7cc8e7009fff
Create Date: 2026-01-03 07:59:19.306793

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "856836e32e99"
down_revision: Union[str, Sequence[str], None] = "7cc8e7009fff"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
