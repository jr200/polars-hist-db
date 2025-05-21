from typing import List


def flatten_list(lst) -> List[str]:
    if not isinstance(lst, list):
        return [lst]
    result = []
    for item in lst:
        result.extend(flatten_list(item))
    return result
