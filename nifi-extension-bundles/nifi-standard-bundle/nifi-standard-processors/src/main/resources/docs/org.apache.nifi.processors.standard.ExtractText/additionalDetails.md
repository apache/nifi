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

# ExtractText

## Usage Information

The Extract Text processor provides different results based on whether named capture groups are enabled.

## Example

Here is a like for like example that illustrates this.

#### Data

        `foo\r\nbar1\r\nbar2\r\nbar3\r\nhello\r\nworld\r\n`

### Without named capture groups

#### Configuration

| Property Name | Property Value                                  |
|---------------|-------------------------------------------------|
| regex.result1 | (?s)(.\*)                                       |
| regex.result2 | (?s).\*(bar1).\*                                |
| regex.result3 | (?s).\*?(bar\\\\d).\*                           |
| regex.result4 | (?s).\*?(?:bar\\\\d).\*?(bar\\\\d).\*?(bar3).\* |
| regex.result5 | (?s).\*(bar\\\\d).\*                            |
| regex.result6 | (?s)^(.\*)$                                     |
| regex.result7 | (?s)(XXX)                                       |

#### Results

| Attribute Name  | Attribute Value                                           |
|-----------------|-----------------------------------------------------------|
| regex.result1   | `   foo\r\nbar1\r\nbar2\r\nbar3\r\nhello\r\nworld\r\n   ` |
| regex.result2   | bar1                                                      |
| regex.result3   | bar1                                                      |
| regex.result4   | bar2                                                      |
| regex.result4.0 | `   foo\r\nbar1\r\nbar2\r\nbar3\r\nhello\r\nworld\r\n   ` |
| regex.result4.1 | bar2                                                      |
| regex.result4.2 | bar3                                                      |
| regex.result5   | bar3                                                      |
| regex.result6   | `   foo\r\nbar1\r\nbar2\r\nbar3\r\nhello\r\nworld\r\n   ` |
| regex.result7   |                                                           |

### With named capture groups

#### Configuration

| Property Name              | Property Value                                            |
|----------------------------|-----------------------------------------------------------|
| Enable named group support | True                                                      |
| regex.result1              | (?s)(?<ALL>.\*                                            |
| regex.result2              | (?s).\*(?<BAR1>bar1).\*                                   |
| regex.result3              | (?s).\*?(?<BAR1>bar\\d).\*                                |
| regex.result4              | (?s).\*?(?:bar\\d).\*?(?<BAR2>bar\\d).\*?(?<BAR3>bar3).\* |
| regex.result5              | (?s).\*(?<BAR3>bar\\d).\*                                 |
| regex.result6              | (?s)^(?<ALL>.\*)$                                         |
| regex.result7              | (?s)(?<MISS>XXX)                                          |

#### Results

| Attribute Name     | Attribute Value                                           |
|--------------------|-----------------------------------------------------------|
| regex.result1      | `   foo\r\nbar1\r\nbar2\r\nbar3\r\nhello\r\nworld\r\n   ` |
| regex.result2.BAR1 | bar1                                                      |
| regex.result3.BAR1 | bar1                                                      |
| regex.result4.BAR2 | bar2                                                      |
| regex.result4.BAR2 | bar2                                                      |
| regex.result4.BAR3 | bar3                                                      |
| regex.result5.BAR3 | bar3                                                      |
| regex.result6.ALL  | `   foo\r\nbar1\r\nbar2\r\nbar3\r\nhello\r\nworld\r\n   ` |
| regex.result7.MISS |                                                           |