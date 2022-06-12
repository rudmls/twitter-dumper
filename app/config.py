from dataclasses import dataclass
from datetime import datetime


@dataclass
class TwitterConfig:
    bearer_token: str


@dataclass
class MongoDbConfig:
    username: str
    password: str
    host: str


@dataclass
class TweetsConfig:
    start_time: datetime
    max_number: int


@dataclass
class DumperConfig:
    username: str
    user_tweets: TweetsConfig
    search_tweets: TweetsConfig


@dataclass
class Config:
    twitter: TwitterConfig
    mongo_db: MongoDbConfig
    dumper: DumperConfig
