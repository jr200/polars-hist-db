from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Literal, Optional, Union

from sqlalchemy import Select, Subquery, Table
from sqlalchemy.dialects import mysql


@dataclass
class TimeHint:
    mode: Literal["none", "all", "asof", "span"] = "none"
    all: bool = False
    asof_utc: Optional[datetime] = field(default=None)
    history_span: Optional[timedelta] = field(default=None)

    def build(self) -> Optional[str]:
        match self.mode:
            case "none":
                return None

            case "all":
                return "FOR SYSTEM_TIME ALL"

            case "asof":
                assert isinstance(self.asof_utc, datetime)
                time_hint = f"FOR SYSTEM_TIME AS OF '{self.asof_utc.isoformat()}'"
                return time_hint

            case "span":
                assert isinstance(self.asof_utc, datetime)
                assert isinstance(self.history_span, timedelta)

                start_date_utc = self.asof_utc - self.history_span
                time_hint = f"FOR SYSTEM_TIME BETWEEN '{start_date_utc.isoformat()}' AND '{self.asof_utc.isoformat()}'"
                return time_hint

        raise ValueError(f"invalid TimeHint mode: {self.mode}")

    def apply(self, query: Select, tbl: Union[Table, Subquery]) -> Select:
        hint = self.build()
        if not hint:
            return query

        compiled = query.with_hint(tbl, hint).compile(dialect=mysql.dialect())
        assert isinstance(compiled.statement, Select)
        result = compiled.statement

        return result
