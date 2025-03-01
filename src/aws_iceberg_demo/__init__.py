import os
from typing import Literal, cast

from rich.console import Console

_tutorial_type_env = os.getenv("TUTORIAL_TYPE", "aws")
if _tutorial_type_env not in ("aws", "local"):
    raise ValueError(f"Unknown TUTORIAL_TYPE: {_tutorial_type_env}")

TUTORIAL_TYPE: Literal["aws", "local"] = cast(Literal["aws", "local"], _tutorial_type_env)

console = Console()
