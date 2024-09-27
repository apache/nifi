<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# JsonQueryElasticsearch

This processor is intended for use with the Elasticsearch JSON DSL and Elasticsearch 5.X and newer. It is designed to be
able to take a JSON query (e.g. from Kibana) and execute it as-is against an Elasticsearch cluster. Like all processors
in the "restapi" bundle, it uses the official Elastic client APIs, so it supports leader detection.

The query JSON to execute can be provided either in the Query configuration property or in the content of the flowfile.
If the Query Attribute property is configured, the executed query JSON will be placed in the attribute provided by this
property.

Additionally, search results and aggregation results can be split up into multiple flowfiles. Aggregation results will
only be split at the top level because nested aggregations lose their context (and thus lose their value) if separated
from their parent aggregation. The following is an example query that would be accepted:

```json
{
  "query": {
    "match": {
      "restaurant.keyword": "Local Pizzaz FTW Inc"
    }
  },
  "aggs": {
    "weekly_sales": {
      "date_histogram": {
        "field": "date",
        "interval": "week"
      },
      "aggs": {
        "items": {
          "terms": {
            "field": "product",
            "size": 10
          }
        }
      }
    }
  }
}
```

