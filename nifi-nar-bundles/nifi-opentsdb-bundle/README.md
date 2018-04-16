# nifi-opentsdb-bundle

## Verbs

* POST

## Requests

### The fields and examples below refer to the default JSON serializer.

|    Name    |  Data Type   |   Required    |   Description     |   RW    |   Example    |
| :--------- | :----------- | :------------ | :---------------- | :-----: | :----------: |  
|metric   |String	                |Required   |The name of the metric you are storing.|W|sys.cpu.nice|
|timestamp|Integer                  |Required   |A Unix epoch style timestamp in seconds or milliseconds.The timestamp must not contain non-numeric characters.|W|1365465600|
|value    |Integer, Float, String   |Required   |The value to record for this data point. It may be quoted or not quoted and must conform to the OpenTSDB value rules: ../../user_guide/writing|W|42.5|
|tags     |Map	                    |Required   |A map of tag name/tag value pairs. At least one pair must be supplied.|W|{"host":"web01"}|

### Example Single Data Point Put

You can supply a single data point in a request:

```json
{
    "metric": "sys.cpu.nice",
    "timestamp": 1346846400,
    "value": 18,
    "tags": {
       "host": "web01",
       "dc": "lga"
    }
}
```

### Example Multiple Data Point Put

Multiple data points must be encased in an array:

```json
[
    {
        "metric": "sys.cpu.nice",
        "timestamp": 1346846400,
        "value": 18,
        "tags": {
           "host": "web01",
           "dc": "lga"
        }
    },
    {
        "metric": "sys.cpu.nice",
        "timestamp": 1346846400,
        "value": 9,
        "tags": {
           "host": "web02",
           "dc": "lga"
        }
    }
]
```