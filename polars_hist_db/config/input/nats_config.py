from dataclasses import dataclass
from typing import Dict, Any, List


@dataclass
class NatsConfig:
    servers: List[str]
    options: Dict[str, Any]

    def __post_init__(self):
        if self.options is None:
            self.options = dict()
