# Big data challenge


## Some mongodb command

```
db.stream_search_tweets.find({}, {data:1}).limit(2)
```

```
db.stream_search_tweets.find({}, {_id:0, "includes.users":1}).limit(1)
```

```
db.search_tweets.find({}, {_id:0, created_at:1}).sort({created_at:1}).limit(1)
```
