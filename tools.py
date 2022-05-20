from typing import List
import enum

def chunk(lst: List, n: int) -> List[List]:
    n = max(1, n)
    return [lst[i:i + n] for i in range(0, len(lst), n)]



class StatusCodes(enum.Enum):
    InQueue = 0
    InProcess = 1
    ParsedFromPost = 10
    ParsedFromCommentSuccess = 11
    NotFoundOrDeleted = 20
    AccessToPostCommentsDenied = 22