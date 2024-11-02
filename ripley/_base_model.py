from dataclasses import dataclass
from typing import Any


@dataclass
class BaseModel:
    def __eq__(self, __value: Any):
        return isinstance(__value, self.__class__) and self.__dict__ == __value.__dict__


