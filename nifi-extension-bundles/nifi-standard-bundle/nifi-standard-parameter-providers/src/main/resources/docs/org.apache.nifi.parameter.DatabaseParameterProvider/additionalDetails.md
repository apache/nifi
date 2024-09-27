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

# DatabaseParameterProvider

## Providing Parameters from a Database

The DatabaseParameterProvider at its core maps database rows to Parameters, specified by a Parameter Name Column and
Parameter Value Column. The Parameter Group name must also be accounted for, and may be specified in different ways
using the Parameter Grouping Strategy.

Before discussing the actual configuration, note that in some databases, the words 'PARAMETER', 'PARAMETERS', 'GROUP',
and even 'VALUE' are reserved words. If you choose a column name that is a reserved word in the database you are using,
make sure to quote it per the database documentation.

Also note that you should use the preferred table name and column name case for your database. For example, Postgres
prefers lowercase table and column names, while Oracle prefers capitalized ones. Choosing the appropriate case can avoid
unexpected issues in configuring your DatabaseParameterProvider.

The default configuration uses a fully column-based approach, with the Parameter Group Name also specified by columns in
the same table. An example of a table using this configuration would be:

**PARAMETER\_CONTEXTS**

| PARAMETER\_NAME | PARAMETER\_VALUE | PARAMETER\_GROUP |
|-----------------|------------------|------------------|
| param.foo       | value-foo        | group\_1         |
| param.bar       | value-bar        | group\_1         |
| param.one       | value-one        | group\_2         |
| param.two       | value-two        | group\_2         |

Table 1: Database table example with Grouping Strategy = Column

In order to use the data from this table, set the following Properties:

* **Parameter Grouping Strategy** - Column
* **Table Name** - PARAMETER\_CONTEXTS
* **Parameter Name Column** - PARAMETER\_NAME
* **Parameter Value Column** - PARAMETER\_VALUE
* **Parameter Group Name Column** - PARAMETER\_GROUP

Once fetched, the parameters in this example will look like this:

Parameter Group **group\_1**:

* param.foo - value-foo
* param.bar - value-bar

Parameter Group **group\_2**:

* param.one - value-one
* param.two - value-two

### Grouping Strategy

The default Grouping Strategy is by Column, which allows you to specify the parameter Group name explicitly in the
Parameter Group Column. Note that if the value in this column is NULL, an exception will be thrown.

The other Grouping Strategy is by Table, which maps each table to a Parameter Group and sets the Parameter Group Name to
the table name. In this Grouping Strategy, the Parameter Group Column is not used. An example configuration using this
strategy would be:

* **Parameter Grouping Strategy** - Table
* **Table Names** - KAFKA, S3
* **Parameter Name Column** - PARAMETER\_NAME
* **Parameter Value Column** - PARAMETER\_VALUE

An example of some tables that may be used with this strategy:

**KAFKA**

| PARAMETER\_NAME | PARAMETER\_VALUE      |
|-----------------|-----------------------|
| brokers         | http://localhost:9092 |
| topic           | my-topic              |
| password        | my-password           |

Table 2: 'KAFKA' Database table example with Grouping Strategy = Table

**S3**

| PARAMETER\_NAME   | PARAMETER\_VALUE |
|-------------------|------------------|
| bucket            | my-bucket        |
| secret.access.key | my-key           |

Table 3: 'S3' Database table example with Grouping Strategy = Table

Once fetched, the parameters in this example will look like this:

Parameter Group **KAFKA**:

* brokers - http://localhost:9092
* topic - my-topic
* password - my-password

Parameter Group **S3**:

* bucket - my-bucket
* secret.access.key - my-key

### Filtering rows

If you need to include only some rows in a table as parameters, you can use the 'SQL WHERE clause' property. An example
of this is as follows:

* **Parameter Grouping Strategy** - Table
* **Table Names** - KAFKA, S3
* **Parameter Name Column** - PARAMETER\_NAME
* **Parameter Value Column** - PARAMETER\_VALUE
* **SQL WHERE clause** - OTHER\_COLUMN = 'my-parameters'

Here we are assuming there is another column, 'OTHER\_COLUMN' in both the KAFKA and S3 tables. Only rows whose '
OTHER\_COLUMN' value is 'my-parameters' will then be fetched from these tables.