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

# IPLookupService

The IPLookupService is powered by a MaxMind database and can return several different types of enrichment information
about a given IP address. Below is the schema of the Record that is returned by this service (in Avro Schema format).
The schema is for a single record that consists of several fields: `geo`, `isp`, `domainName`, `connectionType`, and
`anonymousIp`. Each of these fields is nullable and will be populated only if the IP address that is searched for has
the relevant information in the MaxMind database and if the Controller Service is configured to return such information.
Because each of the fields requires a separate lookup in the database, it is advisable to retrieve only those fields
that are of value.

```json
{
  "name": "enrichmentRecord",
  "namespace": "nifi",
  "type": "record",
  "fields": [
    {
      "name": "geo",
      "type": [
        "null",
        {
          "name": "cityGeo",
          "type": "record",
          "fields": [
            {
              "name": "city",
              "type": [
                "null",
                "string"
              ]
            },
            {
              "name": "accuracy",
              "type": [
                "null",
                "int"
              ],
              "doc": "The radius, in kilometers, around the given location, where the IP address is believed to be"
            },
            {
              "name": "metroCode",
              "type": [
                "null",
                "int"
              ]
            },
            {
              "name": "timeZone",
              "type": [
                "null",
                "string"
              ]
            },
            {
              "name": "latitude",
              "type": [
                "null",
                "double"
              ]
            },
            {
              "name": "longitude",
              "type": [
                "null",
                "double"
              ]
            },
            {
              "name": "country",
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "country",
                  "fields": [
                    {
                      "name": "name",
                      "type": "string"
                    },
                    {
                      "name": "isoCode",
                      "type": "string"
                    }
                  ]
                }
              ]
            },
            {
              "name": "subdivisions",
              "type": {
                "type": "array",
                "items": {
                  "type": "record",
                  "name": "subdivision",
                  "fields": [
                    {
                      "name": "name",
                      "type": "string"
                    },
                    {
                      "name": "isoCode",
                      "type": "string"
                    }
                  ]
                }
              }
            },
            {
              "name": "continent",
              "type": [
                "null",
                "string"
              ]
            },
            {
              "name": "postalCode",
              "type": [
                "null",
                "string"
              ]
            }
          ]
        }
      ]
    },
    {
      "name": "isp",
      "type": [
        "null",
        {
          "name": "ispEnrich",
          "type": "record",
          "fields": [
            {
              "name": "name",
              "type": [
                "null",
                "string"
              ]
            },
            {
              "name": "organization",
              "type": [
                "null",
                "string"
              ]
            },
            {
              "name": "asn",
              "type": [
                "null",
                "int"
              ]
            },
            {
              "name": "asnOrganization",
              "type": [
                "null",
                "string"
              ]
            }
          ]
        }
      ]
    },
    {
      "name": "domainName",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "name": "connectionType",
      "type": [
        "null",
        "string"
      ],
      "doc": "One of 'Dialup', 'Cable/DSL', 'Corporate', 'Cellular'"
    },
    {
      "name": "anonymousIp",
      "type": [
        "null",
        {
          "name": "anonymousIpType",
          "type": "record",
          "fields": [
            {
              "name": "anonymous",
              "type": "boolean"
            },
            {
              "name": "anonymousVpn",
              "type": "boolean"
            },
            {
              "name": "hostingProvider",
              "type": "boolean"
            },
            {
              "name": "publicProxy",
              "type": "boolean"
            },
            {
              "name": "torExitNode",
              "type": "boolean"
            }
          ]
        }
      ]
    }
  ]
}
```

While this schema is fairly complex, it is a single record with 5 fields. This makes it quite easy to update an existing
schema to allow for this record, by adding a new field to an existing schema and pasting in the schema above as the
type.

For example, suppose that we have an existing schema that is as simple as:

```json
{
  "name": "ipRecord",
  "namespace": "nifi",
  "type": "record",
  "fields": [
    {
      "name": "ip",
      "type": "string"
    }
  ]
}
```

Now, let's suppose that we want to add a new field named `enrichment` to the above schema. Further, let's say that we
want the new `enrichment` field to be nullable. We can do so by copying and pasting our enrichment schema from above
thus:

```json
{
  "name": "ipRecord",
  "namespace": "nifi",
  "type": "record",
  "fields": [
    {
      "name": "ip",
      "type": "string"
    },
    {
      "name": "enrichment",
      "type": [
        "null",
        <Paste Enrichment Schema Here>
      ]
    }
  ]
}
```