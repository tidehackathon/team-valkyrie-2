# REST API

The following paragraphs describe the direct use of the API.

##  Ping

The following route is used to ping the API:

#### Request:

```bash
GET /
```

#### Response:


```bash
HTTP 200 OK
{
    "message": "ok"
}
```

## Features

The following route is used to extract features from text:

#### Request:

```bash
POST /features
{
    "WH fabricates allegations to serve as proof that Russians fabricated allegations to serve as proof of pretext to invade Ukraine. Russia denies allegation. US denies fabricating allegation but provides no evidence beyond alleging that it is so\nhttps://t.co/QG8ElqIMdF\nWhat a \ud83c\udfaa\ud83e\udd21\ud83e\udd2a"
}
```

#### Responses:

```bash
HTTP 200 OK
{
  "input": "WH fabricates allegations to serve as proof that Russians fabricated allegations to serve as proof of pretext to invade Ukraine. Russia denies allegation. US denies fabricating allegation but provides no evidence beyond alleging that it is so\nhttps://t.co/QG8ElqIMdF\nWhat a \ud83c\udfaa\ud83e\udd21\ud83e\udd2a",
  "lemmatized": [
    "WH",
    "fabricate",
    "allegation",
    ...
    "what",
  ],
  "entities": [
    {
      "text": "WH",
      "start_char": 0,
      "end_char": 2,
      "label": "ORG"
    },
    {
      "text": "Russians",
      "start_char": 49,
      "end_char": 57,
      "label": "NORP"
    },
    {
      "text": "Ukraine",
      "start_char": 120,
      "end_char": 127,
      "label": "GPE"
    },
    {
      "text": "Russia",
      "start_char": 129,
      "end_char": 135,
      "label": "GPE"
    },
    {
      "text": "US",
      "start_char": 155,
      "end_char": 157,
      "label": "GPE"
    },
    {
      "text": "\ud83c\udfaa\ud83e\udd21",
      "start_char": 274,
      "end_char": 276,
      "label": "DATE"
    }
  ],
  "words": {
    "WH": {
      "count": 1,
      "pos": "PROPN",
      "is_stop": false
    },
    "fabricate": {
      "count": 2,
      "pos": "VERB",
      "is_stop": false
    },
    "allegation": {
      "count": 4,
      "pos": "NOUN",
      "is_stop": false
    },
    ...
  }
}
```