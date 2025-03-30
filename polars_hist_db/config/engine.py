from dataclasses import dataclass, asdict
from typing import Dict, Optional


@dataclass
class DbEngineConfig:
    hostname: str
    backend: str = "mariadb"
    port: int = 3306
    username: Optional[str] = None
    password: Optional[str] = None

    def to_dict(self) -> Dict:
        return asdict(self)
