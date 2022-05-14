from typing import List

def chunk(lst: List, n: int) -> List[List]:
    n = max(1, n)
    return [lst[i:i + n] for i in range(0, len(lst), n)]
