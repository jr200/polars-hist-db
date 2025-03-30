import re
from typing import List, Sequence


def parse_col_spec(col_expr: str):
    pattern = r"^(?P<col_name>[^<\s!]*)\s*([<]?\s*)(?P<col_def_name>[^\s!]*)(?P<col_is_required>[!]?)$"
    m = re.match(pattern, col_expr)
    if m is None:
        raise ValueError(f"failed to column expression: '{col_expr}'")

    d = m.groupdict()
    col_name = d["col_name"]
    col_def_name = d.get("col_def_name") if d.get("col_def_name") else col_name
    col_required = d.get("col_is_required", False)

    return col_name, col_def_name, col_required


def parse_col_exprs(exprs: Sequence[str]):
    for col_expr in flatten_list(exprs):
        col_name, col_def_name, col_is_required = parse_col_spec(col_expr)
        yield col_name, col_def_name, col_is_required


def flatten_list(lst) -> List[str]:
    if not isinstance(lst, list):
        return [lst]
    result = []
    for item in lst:
        result.extend(flatten_list(item))
    return result
