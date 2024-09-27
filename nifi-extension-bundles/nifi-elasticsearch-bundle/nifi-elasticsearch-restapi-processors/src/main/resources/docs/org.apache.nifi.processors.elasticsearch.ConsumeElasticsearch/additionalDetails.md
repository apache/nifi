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

# SearchElasticsearch

This processor is intended for use with the Elasticsearch JSON DSL and Elasticsearch 5.X and newer. It is designed to be
able to create a JSON query using input properties and execute it against an Elasticsearch cluster in a paginated
manner. Like all processors in the "restapi" bundle, it uses the official Elastic client APIs, so it supports leader
detection.

The query is paginated in Elasticsearch using one of the available methods - "Scroll" or "Search After" (optionally with
a "Point in Time" for Elasticsearch 7.10+ with XPack enabled). The number of results per page can be controlled using
the _size_ property.

Results will be sorted on the field that is to be tracked, with the _sort order_ set as a property.

Search results and aggregation results can be split up into multiple flowfiles. Aggregation results will only be split
at the top level because nested aggregations lose their context (and thus lose their value) if separated from their
parent aggregation. Additionally, the results from all pages can be combined into a single flowfile (but the processor
will only load each page of data into memory at any one time).

The following is an example query that would be created for tracking a "@timestamp" field:

```json
{
  "query": {
    "size": 10000,
    "sort": {
      "@timestamp": "desc"
    },
    "bool": {
      "filter": [
        {
          "range": {
            "@timestamp": {
              "gt": "2023-09-01"
            }
          }
        }
      ]
    }
  }
}
```

Additional "filter" entries can be added as a JSON string in the _query filter_ property, for example:

```json
[
  {
    "term": {
      "department": "accounts"
    }
  },
  {
    "term": {
      "title.keyword": "Annual Report"
    }
  }
]
```

## Query Pagination Across Processor Executions

This processor runs on a schedule in order to execute the same query repeatedly. Once a paginated query has been
initiated within Elasticsearch, this processor will continue to retrieve results for that same query until no further
results are available. After that point, a new paginated query will be initiated using the same Query JSON, but with
the "range" filter query's "gt" field set to the last value obtained from previous results.

If the results are "Combined" from this processor, then the paginated query will run continually within a single
invocation until no more results are available (then the processor will start a new paginated query upon its next
invocation). If the results are "Split" or "Per Page", then each invocation of this processor will retrieve the next
page of results until either there are no more results or the paginated query expires within Elasticsearch.

## Resetting Queries / Clearing Processor State

Cluster State is used to track the progress of a paginated query within this processor. If there is need to restart the
query completely or change the processor configuration after a paginated query has already been started, be sure to 
"Clear State" of the processor once it has been stopped and before restarting.

Note that clearing the processor's state will lose details of where the processor was up to with tracking documents
retrieved. Update the "Initial Value" with the appropriate value before restarting the processor to continue with where
it was up to.

## Duplicate Results

This processor does not attempt to de-duplicate results between queries, for example if the same query runs twice and 
(some or all of) the results are identical, the output will contain these same results for both invocations. This might
happen if the NiFi Primary Node changes while a page of data is being retrieved, or if the processor state is cleared,
then the processor is restarted.