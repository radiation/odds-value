from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "ee62d5f6f6df"
down_revision: Union[str, Sequence[str], None] = "2b3e9c920e0c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


providerenum = sa.Enum("api_sports", "nflverse", "odds_api", name="providerenum")


def upgrade() -> None:
    bind = op.get_bind()

    # 1) Create enum type first
    providerenum.create(bind, checkfirst=True)

    # 2) Add column using the enum type
    op.add_column(
        "games",
        sa.Column(
            "provider",
            providerenum,
            nullable=False,
            server_default="api_sports",
        ),
    )

    # 3) Update constraint (keep your existing behavior)
    op.drop_constraint(op.f("uq_games_league_provider_game_id"), "games", type_="unique")
    op.create_unique_constraint(
        "uq_game_provider_ext_id", "games", ["provider", "provider_game_id"]
    )

    # 4) Optional: remove default after backfill
    op.alter_column("games", "provider", server_default=None)


def downgrade() -> None:
    bind = op.get_bind()

    op.drop_constraint("uq_game_provider_ext_id", "games", type_="unique")
    op.create_unique_constraint(
        op.f("uq_games_league_provider_game_id"),
        "games",
        ["league_id", "provider_game_id"],
        postgresql_nulls_not_distinct=False,
    )
    op.drop_column("games", "provider")

    # Drop enum type last
    providerenum.drop(bind, checkfirst=True)
