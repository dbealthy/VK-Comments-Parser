from dataclasses import dataclass
import enum

@dataclass
class Comment:
    id: int
    user_id: int
    post_id: int
    author_id: int
    parent_id: int
    author_link: str
    comment_link: str
    likes: int
    text: str
    date: str

    def __eq__(self, other):
        return self.id == other.id

    def __hash__(self):
        return self.id

    def values(self):
        return self.post_id, self.author_id, self.id, self.author_link, self.comment_link, self.text, self.likes, self.date, self.parent_id
    

@dataclass
class Author:
    id: int
    link: str
    screen_name: str
    name: str
    bdate: str
    sex: str
    location: str
    photo_link: str

    def values(self):
        return self.id, self.link, self.screen_name, self.name, self.bdate, self.sex, self.location, self.photo_link

@dataclass
class PostLog:
    p_id: int
    count_comments: int
    status_code: int

class Codes(enum.Enum):
    Success = 10
    ParsedFromCommentSuccess = 11
    PostNotFoundOrDeleted = 20
