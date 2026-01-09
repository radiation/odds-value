from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd  # type: ignore


@dataclass(frozen=True, slots=True)
class NflverseClient:
    """
    Thin wrapper around nfl_data_py. Imports are localized so import-time failures
    (pandas wheels, etc.) don't break module import elsewhere.
    """

    def import_schedules(self, years: Iterable[int]) -> pd.DataFrame:
        import nfl_data_py as nfl  # type: ignore
        import pandas as pd

        df = nfl.import_schedules(years=list(years))
        if not isinstance(df, pd.DataFrame):
            raise TypeError("nfl.import_schedules did not return a DataFrame")
        return df

    def import_pbp(self, years: Iterable[int], *, columns: list[str]) -> pd.DataFrame:
        import nfl_data_py as nfl  # local
        import pandas as pd  # local

        df = nfl.import_pbp_data(
            years=list(years),
            columns=columns,
            downcast=True,
        )
        if not isinstance(df, pd.DataFrame):
            raise TypeError("nfl.import_pbp_data did not return a DataFrame")
        return df
