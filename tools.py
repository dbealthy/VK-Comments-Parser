from typing import List
import enum

def chunk(lst: List, n: int) -> List[List]:
    n = max(1, n)
    return [lst[i:i + n] for i in range(0, len(lst), n)]



class StatusCodes(enum.Enum):
    InQueue = 0                      # Task waits untill it is taken
    InProcess = 1                    # Task is taken from the table and is being preccsed
    Failed = 5                       # Program was interrapted somehow (error or keyboard interrapt)
    ParsedFromPost = 10              # Successfully parsed from a post
    ParsedFromComment = 11           # Successfully parsed from a comment
    NotFoundOrDeleted = 20           # Post or comment was not found
    AccessToPostCommentsDenied = 22  # There is not access to comment section in post/comment