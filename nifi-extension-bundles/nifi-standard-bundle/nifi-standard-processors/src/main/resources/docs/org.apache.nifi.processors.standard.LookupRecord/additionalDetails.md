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

# LookupRecord

LookupRecord makes use of the NiFi RecordPath Domain-Specific Language (DSL) to allow the user to
indicate which field(s), depending on the Record Update Strategy, in the Record should be updated. The Record will be
updated using the value returned by the provided Lookup Service.

### Record Update Strategy - Use Property

In this case, the user should add, to the Processor's configuration, as much User-defined Properties as required by the
Lookup Service to form the lookup coordinates. The name of the properties should match the names expected by the Lookup
Service.

The field evaluated using the path configured in the "Result RecordPath" property will be the field updated with the
value returned by the Lookup Service.

Let's assume a Simple Key Value Lookup Service containing the following key/value pairs:

`FR => France CA => Canada`

Let's assume the following JSON with three records as input:

```json
[
  {
    "country": null,
    "code": "FR"
  },
  {
    "country": null,
    "code": "CA"
  },
  {
    "country": null,
    "code": "JP"
  }
]
```

The processor is configured with "Use Property" as "Record Update Strategy", the "Result RecordPath" is configured
with "/country" and a user-defined property is added with the name "key" (as required by this Lookup Service) and the
value "/code".

When triggered, the processor will look for the value associated to the "/code" path and will use the value as the "key"
of the Lookup Service. The value returned by the Lookup Service will be used to update the value corresponding to "
/country". With the above examples, it will produce:

```json
[
  {
    "country": "France",
    "code": "FR"
  },
  {
    "country": "Canada",
    "code": "CA"
  },
  {
    "country": null,
    "code": "JP"
  }
]
```

### Record Update Strategy - Replace Existing Values

With this strategy, the "Result RecordPath" property will be ignored and the configured Lookup Service must be a single
simple key lookup service. For each user-defined property, the value contained in the field corresponding to the record
path will be used as the key in the Lookup Service and will be replaced by the value returned by the Lookup Service. It
is possible to configure multiple dynamic properties to update multiple fields in one execution. This strategy only
supports simple types replacements (strings, integers, etc).

Since this strategy allows in-place replacement, it is possible to use Record Paths for fields contained in arrays.

Let's assume a Simple Key Value Lookup Service containing the following key/value pairs:

`FR => France CA => Canada fr => French en => English`

Let's assume the following JSON with two records as input:

```json
[
  {
    "locales": [
      {
        "region": "FR",
        "language": "fr"
      },
      {
        "region": "US",
        "language": "en"
      }
    ]
  },
  {
    "locales": [
      {
        "region": "CA",
        "language": "fr"
      },
      {
        "region": "JP",
        "language": "ja"
      }
    ]
  }
]
```

The processor is configured with "Replace Existing Values" as "Record Update Strategy", two user-defined properties are
added: "region" => "/locales\[\*\]/region" and "language => "/locales\[\*\]/language"..

When triggered, the processor will loop over the user-defined properties. First, it'll search for the fields
corresponding to "/locales\[\*\]/region", for each value from the record, the value will be used as the key with the
Lookup Service and the value will be replaced by the result returned by the Lookup Service. Example: the first region
is "FR" and this key is associated to the value "France" in the Lookup Service, so the value "FR" is replaced by "
France" in the record. With the above examples, it will produce:

```json
[
  {
    "locales": [
      {
        "region": "France",
        "language": "French"
      },
      {
        "region": "US",
        "language": "English"
      }
    ]
  },
  {
    "locales": [
      {
        "region": "Canada",
        "language": "French"
      },
      {
        "region": "JP",
        "language": "ja"
      }
    ]
  }
]
```