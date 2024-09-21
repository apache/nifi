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

# ForkRecord

ForkRecord allows the user to fork a record into multiple records. To do that, the user must specify one or
multiple RecordPath (as dynamic properties of the processor) pointing to a field of type ARRAY containing RECORD elements.

The processor accepts two modes:

* Split mode - in this mode, the generated records will have the same schema as the input. For every element in the
  array, one record will be generated and the array will only contain this element.
* Extract mode - in this mode, the generated records will be the elements contained in the array. Besides, it is also
  possible to add in each record all the fields of the parent records from the root level to the record element being
  forked. However it supposes the fields to add are defined in the schema of the Record Writer controller service.

## Examples

### EXTRACT mode

To better understand how this Processor works, we will lay out a few examples. For the sake of these examples, let's
assume that our input data is JSON formatted and looks like this:

```json
[
  {
    "id": 1,
    "name": "John Doe",
    "address": "123 My Street",
    "city": "My City",
    "state": "MS",
    "zipCode": "11111",
    "country": "USA",
    "accounts": [
      {
        "id": 42,
        "balance": 4750.89
      },
      {
        "id": 43,
        "balance": 48212.38
      }
    ]
  },
  {
    "id": 2,
    "name": "Jane Doe",
    "address": "345 My Street",
    "city": "Her City",
    "state": "NY",
    "zipCode": "22222",
    "country": "USA",
    "accounts": [
      {
        "id": 45,
        "balance": 6578.45
      },
      {
        "id": 46,
        "balance": 34567.21
      }
    ]
  }
]
```

#### Example 1 - Extracting without parent fields

For this case, we want to create one record per `account` and we don't care about the other fields. We'll add a dynamic
property "path" set to `/accounts`. The resulting flow file will contain 4 records and will look like (assuming the
Record Writer schema is correctly set):

```json
[
  {
    "id": 42,
    "balance": 4750.89
  },
  {
    "id": 43,
    "balance": 48212.38
  },
  {
    "id": 45,
    "balance": 6578.45
  },
  {
    "id": 46,
    "balance": 34567.21
  }
]
```

#### Example 2 - Extracting with parent fields

Now, if we set the property "Include parent fields" to true, this will recursively include the parent fields into the
output records assuming the Record Writer schema allows it. In case multiple fields have the same name (like we have in
this example for `id`), the child field will have the priority over all the parent fields sharing the same name. In this
case, the `id` of the array `accounts` will be saved in the forked records. The resulting flow file will contain 4
records and will look like:

```json
[
  {
    "name": "John Doe",
    "address": "123 My Street",
    "city": "My City",
    "state": "MS",
    "zipCode": "11111",
    "country": "USA",
    "id": 42,
    "balance": 4750.89
  },
  {
    "name": "John Doe",
    "address": "123 My Street",
    "city": "My City",
    "state": "MS",
    "zipCode": "11111",
    "country": "USA",
    "id": 43,
    "balance": 48212.38
  },
  {
    "name": "Jane Doe",
    "address": "345 My Street",
    "city": "Her City",
    "state": "NY",
    "zipCode": "22222",
    "country": "USA",
    "id": 45,
    "balance": 6578.45
  },
  {
    "name": "Jane Doe",
    "address": "345 My Street",
    "city": "Her City",
    "state": "NY",
    "zipCode": "22222",
    "country": "USA",
    "id": 46,
    "balance": 34567.21
  }
]
```

#### Example 3 - Multi-nested arrays

Now let's say that the input record contains multi-nested arrays like the below example:

```json
[
  {
    "id": 1,
    "name": "John Doe",
    "address": "123 My Street",
    "city": "My City",
    "state": "MS",
    "zipCode": "11111",
    "country": "USA",
    "accounts": [
      {
        "id": 42,
        "balance": 4750.89,
        "transactions": [
          {
            "id": 5,
            "amount": 150.31
          },
          {
            "id": 6,
            "amount": -15.31
          }
        ]
      },
      {
        "id": 43,
        "balance": 48212.38,
        "transactions": [
          {
            "id": 7,
            "amount": 36.78
          },
          {
            "id": 8,
            "amount": -21.34
          }
        ]
      }
    ]
  }
]
```

``

If we want to have one record per `transaction` for each `account`, then the Record Path should be set to
`/accounts[*]/transactions`. If we have the following schema for our Record Reader:

```json
{
  "type": "record",
  "name": "bank",
  "fields": [
    {
      "name": "id",
      "type": "int"
    },
    {
      "name": "name",
      "type": "string"
    },
    {
      "name": "address",
      "type": "string"
    },
    {
      "name": "city",
      "type": "string"
    },
    {
      "name": "state",
      "type": "string"
    },
    {
      "name": "zipCode",
      "type": "string"
    },
    {
      "name": "country",
      "type": "string"
    },
    {
      "name": "accounts",
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "accounts",
          "fields": [
            {
              "name": "id",
              "type": "int"
            },
            {
              "name": "balance",
              "type": "double"
            },
            {
              "name": "transactions",
              "type": {
                "type": "array",
                "items": {
                  "type": "record",
                  "name": "transactions",
                  "fields": [
                    {
                      "name": "id",
                      "type": "int"
                    },
                    {
                      "name": "amount",
                      "type": "double"
                    }
                  ]
                }
              }
            }
          ]
        }
      }
    }
  ]
}
```

And if we have the following schema for our Record Writer:

```json
{
  "type": "record",
  "name": "bank",
  "fields": [
    {
      "name": "id",
      "type": "int"
    },
    {
      "name": "name",
      "type": "string"
    },
    {
      "name": "address",
      "type": "string"
    },
    {
      "name": "city",
      "type": "string"
    },
    {
      "name": "state",
      "type": "string"
    },
    {
      "name": "zipCode",
      "type": "string"
    },
    {
      "name": "country",
      "type": "string"
    },
    {
      "name": "amount",
      "type": "double"
    },
    {
      "name": "balance",
      "type": "double"
    }
  ]
}
```

Then, if we include the parent fields, we'll have 4 records as below:

```json
[
  {
    "id": 5,
    "name": "John Doe",
    "address": "123 My Street",
    "city": "My City",
    "state": "MS",
    "zipCode": "11111",
    "country": "USA",
    "amount": 150.31,
    "balance": 4750.89
  },
  {
    "id": 6,
    "name": "John Doe",
    "address": "123 My Street",
    "city": "My City",
    "state": "MS",
    "zipCode": "11111",
    "country": "USA",
    "amount": -15.31,
    "balance": 4750.89
  },
  {
    "id": 7,
    "name": "John Doe",
    "address": "123 My Street",
    "city": "My City",
    "state": "MS",
    "zipCode": "11111",
    "country": "USA",
    "amount": 36.78,
    "balance": 48212.38
  },
  {
    "id": 8,
    "name": "John Doe",
    "address": "123 My Street",
    "city": "My City",
    "state": "MS",
    "zipCode": "11111",
    "country": "USA",
    "amount": -21.34,
    "balance": 48212.38
  }
]
```

### SPLIT mode

#### Example

Assuming we have the below data and we added a property "path" set to `/accounts`:

```json
[
  {
    "id": 1,
    "name": "John Doe",
    "address": "123 My Street",
    "city": "My City",
    "state": "MS",
    "zipCode": "11111",
    "country": "USA",
    "accounts": [
      {
        "id": 42,
        "balance": 4750.89
      },
      {
        "id": 43,
        "balance": 48212.38
      }
    ]
  },
  {
    "id": 2,
    "name": "Jane Doe",
    "address": "345 My Street",
    "city": "Her City",
    "state": "NY",
    "zipCode": "22222",
    "country": "USA",
    "accounts": [
      {
        "id": 45,
        "balance": 6578.45
      },
      {
        "id": 46,
        "balance": 34567.21
      }
    ]
  }
]
```

Then we'll get 4 records as below:

```json
[
  {
    "id": 1,
    "name": "John Doe",
    "address": "123 My Street",
    "city": "My City",
    "state": "MS",
    "zipCode": "11111",
    "country": "USA",
    "accounts": [
      {
        "id": 42,
        "balance": 4750.89
      }
    ]
  },
  {
    "id": 1,
    "name": "John Doe",
    "address": "123 My Street",
    "city": "My City",
    "state": "MS",
    "zipCode": "11111",
    "country": "USA",
    "accounts": [
      {
        "id": 43,
        "balance": 48212.38
      }
    ]
  },
  {
    "id": 2,
    "name": "Jane Doe",
    "address": "345 My Street",
    "city": "Her City",
    "state": "NY",
    "zipCode": "22222",
    "country": "USA",
    "accounts": [
      {
        "id": 45,
        "balance": 6578.45
      }
    ]
  },
  {
    "id": 2,
    "name": "Jane Doe",
    "address": "345 My Street",
    "city": "Her City",
    "state": "NY",
    "zipCode": "22222",
    "country": "USA",
    "accounts": [
      {
        "id": 46,
        "balance": 34567.21
      }
    ]
  }
]
```