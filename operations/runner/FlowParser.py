import yaml, os
from jinja2 import Template, DebugUndefined
from typing import Dict, Any, List, Union
from operations.runner.ExecuteParamParser import (
 ScanParamParser, AggregateParamParser, BroadcastJoinParamParser,
 BroadcastJoinPushdownParamParser,
 MergeBFParamParser, SummaryCleanParamParser, CreateBFParamParser,
 HashPartitionParamParser, HashJoinParamParser, RangeJoinParamParser, AdaptiveJoinParamParser
)

PARAM_PARSER_REGISTRY = {
    "scan": ScanParamParser,
    "aggregate": AggregateParamParser,
    "hash_aggregate": AggregateParamParser,
    "merge_bf": MergeBFParamParser,
    "broadcast_join": BroadcastJoinParamParser,
    "broadcast_join_pushdown": BroadcastJoinPushdownParamParser,
    "hash_join": HashJoinParamParser,
    "summary_clean": SummaryCleanParamParser,
    "create_bf": CreateBFParamParser,
    "hash_partition": HashPartitionParamParser,
    "range_join": RangeJoinParamParser,
    "adaptive_join": AdaptiveJoinParamParser,
}


def load_and_render_yaml(path: str, external_vars: dict = None) -> dict:
    with open(path) as f:
        raw_text = f.read()

    # Try to extract static variables (no Jinja2 syntax)
    variables = {}
    lines = raw_text.split('\n')
    in_vars_section = False

    for line in lines:
        stripped = line.strip()

        # Detect variables section
        if stripped == 'variables:':
            in_vars_section = True
            continue

        # Exit variables section if unindented non-empty line
        if in_vars_section and line and not line[0].isspace():
            in_vars_section = False

        # Parse variable if in section and no Jinja2 syntax
        if in_vars_section and ':' in line:
            if '{{' not in line and '{%' not in line and not stripped.startswith('#'):
                try:
                    key, value = line.split(':', 1)
                    key = key.strip()
                    value = value.strip().strip("'\"")

                    # Try to convert to number
                    try:
                        if value.lower() in ('true', 'false'):
                            value = value.lower() == 'true'
                        elif '.' in value:
                            value = float(value)
                        else:
                            value = int(value)
                    except (ValueError, AttributeError):
                        pass

                    variables[key] = value
                except:
                    pass

    # Build context: YAML vars + external vars (external overrides)
    template_context = {
        **variables,
        **(external_vars or {}),
        'range': range,
        'len': len,
        'str': str,
        'int': int,
    }

    # Render Jinja2
    rendered_text = Template(
        raw_text,
        undefined=DebugUndefined,
        trim_blocks=True,
        lstrip_blocks=True
    ).render(**template_context)

    # Parse YAML
    return yaml.safe_load(rendered_text)

class FlowParser:

    def __init__(self, yaml_path: str):
        self.yaml_path = yaml_path
        self.raw = load_and_render_yaml(yaml_path)
        self.name = self.raw.get("name")
        self.description = self.raw.get("description", "")
        self.variables = self.raw.get("variables", {})
        self.steps = self._parse_steps()


    def _load_yaml(self) -> Dict[str, Any]:
        with open(self.yaml_path, "r") as f:
            return yaml.safe_load(f)


    def _parse_steps(self) -> List[Dict[str, Any]]:
        steps = []

        for step in self.raw.get("steps", []):
            step_name = step["name"]
            func = step["func"]

            if func not in PARAM_PARSER_REGISTRY:
                raise ValueError(f"Unsupported func type: {func}")

            param_parser_cls = PARAM_PARSER_REGISTRY[func]
            param_parser = param_parser_cls(step.get("params", {}))

            parsed_step = {
                "name": step_name,
                "func": func,
                "inputs": self._parse_input(step.get("input")),
                "from_previous_tasks": self._parse_input(step.get("from_previous_task")),
                "outputs": self._parse_input(step.get("output")),
                "params": param_parser.parse(),
                "rows_per_worker": step.get("rows_per_worker"),
                "max_size_mb": step.get("max_size_mb"),
                "max_parallel": step.get("max_parallel"),   # per-stage override of the run's max_parallel
            }

            steps.append(parsed_step)

        return steps


    def _parse_input(
        self,
        input_def: Union[str, Dict[str, str], None]
    ) -> Dict[str, str]:

        if input_def is None:
            return {}

        if isinstance(input_def, str):
            return input_def

        if isinstance(input_def, list):
            if not all(isinstance(i, str) for i in input_def):
                raise ValueError(f"Input list must contain only strings: {input_def}")
            return input_def

        if isinstance(input_def, dict):
            normalized = {}
            for k, v in input_def.items():
                if isinstance(v, str):
                    normalized[k] = [v]
                elif isinstance(v, list) and all(isinstance(i, str) for i in v):
                    normalized[k] = v
                else:
                    raise ValueError(
                        f"Invalid input value for key '{k}': {v}"
                    )
            return normalized

        raise ValueError(f"Invalid input format: {input_def}")


    def _estimate_fanout(self, step: Dict[str, Any]) -> int:
        """
        -1  : dynamic (runtime decides N)
         1  : single function
        """
        if "rows_per_worker" in step:
            return -1
        return 1


    def get_flow_info(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "variables": self.variables,
        }

    def get_steps(self) -> List[Dict[str, Any]]:
        return self.steps

    def get_step(self, name: str) -> Dict[str, Any]:
        for step in self.steps:
            if step["name"] == name:
                return step
        raise KeyError(f"Step not found: {name}")
