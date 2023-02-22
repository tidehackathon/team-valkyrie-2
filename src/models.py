import json
from dataclasses import dataclass
from datetime import datetime


@dataclass
class SourcesInfo:
    name: str
    url: str
    description: str = None
    original_id: int = None
    original_timestamp: datetime = None
    source_type_id: int = 1
    details: dict = None
    last_post_timestamp: datetime = None
    posts_total: int = 0
    posts_last_day: int = 0
    posts_last_week: int = 0
    posts_last_month: int = 0
    rating: float = 0.0

    @staticmethod
    def from_df_row(row):
        return SourcesInfo(**row)

    def to_tuple(self):
        elements = []
        for k, v in self.__dict__.items():
            if isinstance(v, dict) or isinstance(v, list):
                elements.append(json.dumps(v))
            else:
                elements.append(v)
        return tuple(elements)


@dataclass
class PostsInfo:
    url: str
    original_timestamp: datetime
    source_id: int = 1
    source_name: str = None
    source_details: dict = None
    content: str = None
    content_rendered: str = None
    lang: str = None
    external_urls: dict = None
    media_details: dict = None
    reply_count: int = 0
    share_count: int = 0
    like_count: int = 0
    quote_count: int = 0
    shared_id: int = None
    shared_details: dict = None
    quoted_id: int = None
    quoted_details: dict = None
    replied_id: int = None
    replied_source_details: dict = None
    mentioned_source_names: dict = None
    mentioned_source_details: dict = None
    hashtags: dict = None
    coordinates: dict = None
    place: dict = None
    original_id: int = None

    @staticmethod
    def from_df_row(row):
        row_formatted = {}

        for k, v in row.to_dict().items():
            v = eval(v) if k not in ['content', 'content_rendered'] and v and isinstance(v, str) and (
                    ('[' == v[0] and ']' == v[-1]) or ('{' == v[0] and '}' == v[-1])) else v

            row_formatted[k] = v
        return PostsInfo(**row_formatted)

    def to_json(self):
        obj_dict = {}
        for k, v in self.__dict__.items():
            if isinstance(v, datetime):
                obj_dict[k] = str(v)
            else:
                obj_dict[k] = v
        return json.dumps(obj_dict)

    def to_tuple(self):
        elements = []
        for k, v in self.__dict__.items():
            if isinstance(v, dict) or isinstance(v, list):
                elements.append(json.dumps(v))
            else:
                elements.append(v)
        return tuple(elements)


@dataclass
class FeaturesInfo:
    id: int
    content: dict

    @staticmethod
    def from_df_row(row):
        return FeaturesInfo(**row)

    def to_tuple(self):
        elements = []
        for k, v in self.__dict__.items():
            if isinstance(v, dict) or isinstance(v, list):
                elements.append(json.dumps(v))
            else:
                elements.append(v)
        return tuple(elements)
