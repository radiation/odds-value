import importlib
import os
from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from odds_value.db.base import Base
from odds_value.db.enums import SportEnum
from odds_value.db.models import League, Season


@pytest.fixture(scope="session", autouse=True)
def _force_test_database_url():
    # Force tests to use sqlite memory, no matter what shell has set
    os.environ["DATABASE_URL"] = "sqlite+pysqlite:///:memory:"
    os.environ.pop("DB_ECHO", None)

    # If settings was already imported somewhere, reload it so the singleton picks up the env var
    try:
        import odds_value.core.config

        importlib.reload(odds_value.core.config)
    except Exception:
        # If not imported yet, nothing to reload
        pass


@pytest.fixture()
def db_session():
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        future=True,
    )

    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine, future=True)
    session = Session()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture()
def league(db_session) -> League:
    league = League(
        provider_league_id="nfl",
        name="NFL",
        sport=SportEnum.NFL,
        country="US",  # optional, you can omit
        is_active=True,  # optional, default exists
    )
    db_session.add(league)
    db_session.commit()
    db_session.refresh(league)
    return league


@pytest.fixture()
def season(db_session, league: League) -> Season:
    season = Season(
        league_id=league.id,
        year=2025,
    )
    db_session.add(season)
    db_session.commit()
    db_session.refresh(season)
    return season


@pytest.fixture()
def now_utc() -> datetime:
    return datetime.now(UTC)
