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

# Amazon Translate

## StartAwsTranslateJob

Amazon Translate is a neural machine translation service for translating text to and from English across a breadth of
supported languages. Powered by deep-learning technologies, Amazon Translate delivers fast, high-quality, and affordable
language translation. It provides a managed, continually trained solution, so you can easily translate company and
user-authored content or build applications that require support across multiple languages. The machine translation
engine has been trained on a wide variety of content across different domains to produce quality translations that serve
any industry need.

### Usage

Amazon ML Processors are implemented to utilize ML services based on the official AWS API Reference. You can find
example json payload in the documentation at the Request Syntax sections. For more details please check the
official [Translate API reference](https://docs.aws.amazon.com/translate/latest/APIReference/welcome.html) With this
processor you will trigger a startTextTranslationJob async call to Translate Service You can define json payload as
property or provide as a flow file content. Property has higher precedence.

JSON payload template - note that it can be simplified with the optional fields,
check [AWS documentation for more details](https://docs.aws.amazon.com/translate/latest/APIReference/API_StartTextTranslationJob.html) -
example:

```json
{
  "ClientToken": "string",
  "DataAccessRoleArn": "string",
  "InputDataConfig": {
    "ContentType": "string",
    "S3Uri": "string"
  },
  "JobName": "string",
  "OutputDataConfig": {
    "EncryptionKey": {
      "Id": "string",
      "Type": "string"
    },
    "S3Uri": "string"
  },
  "ParallelDataNames": [
    "string"
  ],
  "Settings": {
    "Formality": "string",
    "Profanity": "string"
  },
  "SourceLanguageCode": "string",
  "TargetLanguageCodes": [
    "string"
  ],
  "TerminologyNames": [
    "string"
  ]
}
```