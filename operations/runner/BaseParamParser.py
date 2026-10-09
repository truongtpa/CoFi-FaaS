from typing import List, Dict, Any


class BaseParamParser:
    required_fields: List[str] = []
    optional_fields: List[str] = []

    def __init__(self, raw_params: Dict[str, Any]):
        self.raw = raw_params or {}

    def parse(self) -> Dict[str, Any]:
        self._validate_required()
        return self._normalize()

    def _validate_required(self):
        for field in self.required_fields:
            if field not in self.raw:
                raise ValueError(
                    f"Missing required param '{field}' "
                    f"in params: {self.raw}"
                )

    def _normalize(self) -> Dict[str, Any]:
        return self.raw