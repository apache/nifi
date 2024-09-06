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

# ValidateCsv

## Usage Information

The Validate CSV processor is based on the super-csv library and the concept
of [Cell Processors](http://super-csv.github.io/super-csv/cell_processors.html). The corresponding java documentation
can be found [here](http://super-csv.github.io/super-csv/apidocs/org/supercsv/cellprocessor/ift/CellProcessor.html).

The cell processors cannot be nested (except with Optional which gives the possibility to define a CellProcessor for
values that could be null) and must be defined in a comma-delimited string as the Schema property.

The supported cell processors are:

* ParseBigDecimal
* ParseBool
* ParseChar
* ParseDate
* ParseDouble
* ParseInt
* Optional
* DMinMax
* Equals
* ForbidSubStr
* LMinMax
* NotNull
* Null
* RequireHashCode
* RequireSubStr
* Strlen
* StrMinMax
* StrNotNullOrEmpty
* StrRegEx
* Unique
* UniqueHashCode
* IsIncludedIn

Here are some examples:

**Schema property:** Null, ParseDate("dd/MM/yyyy"), Optional(ParseDouble())  
**Meaning:** the input CSV has three columns, the first one can be null and has no specification, the second one must be
a date formatted as expected, and the third one must a double or null (no value).

**Schema property:** ParseBigDecimal(), ParseBool(), ParseChar(), ParseInt(), ParseLong()  
**Meaning:** the input CSV has five columns, the first one must be a big decimal, the second one must be a boolean, the
third one must be a char, the fourth one must be an integer and the fifth one must be a long.

**Schema property:** Equals(), NotNull(), StrNotNullOrEmpty()  
**Meaning:** the input CSV has three columns, all the values of the first column must be equal to each other, all the
values of the second column must be not null, and all the values of the third column are not null/empty string values.

**Schema property:** Strlen(4), StrMinMax(3,5), StrRegex("\[a-z0-9\\\\.\_\]+@\[a-z0-9\\\\.\]+")  
**Meaning:** the input CSV has three columns, all the values of the first column must be 4-characters long, all the
values of the second column must be between 3 and 5 characters (inclusive), and all the values of the last column must
match the provided regular expression (email address).

**Schema property:** Unique(), UniqueHashCode()  
**Meaning:** the input CSV has two columns. All the values of the first column must be unique (all the values are stored
in memory). All the values of the second column must be unique (only hash codes of the input values are stored to ensure
uniqueness).

**Schema property:** ForbidSubStr("test", "tset"), RequireSubStr("test")  
**Meaning:** the input CSV has two columns. None of the values in the first column must contain one of the provided
strings. And all the values of the second column must contain the provided string.