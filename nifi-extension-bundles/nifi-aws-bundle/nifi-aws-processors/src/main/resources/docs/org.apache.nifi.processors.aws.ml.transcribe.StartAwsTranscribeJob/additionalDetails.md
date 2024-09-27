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

# Amazon Transcribe

Automatically convert speech to text

### Usage

Amazon ML Processors are implemented to utilize ML services based on the official AWS API Reference. You can find
example json payload in the documentation at the Request Syntax sections. For more details please check the
official [Transcribe API reference](https://docs.aws.amazon.com/transcribe/latest/APIReference/Welcome.html) With this
processor you will trigger a startTranscriptionJob async call to AWS Transcribe Service. You can define json payload as
property or provide as a flow file content. Property has higher precedence. After the job is triggered the serialized
json response will be written to the output flow file. The awsTaskId attribute will be populated, so it makes it easier
to query job status by the corresponding get job status processor.

JSON payload template - note that these can be simplified with the optional fields,
check [AWS documentation for more details](https://docs.aws.amazon.com/transcribe/latest/APIReference/API_StartTranscriptionJob.html) -
examples:

```json
{
  "ContentRedaction": {
    "PiiEntityTypes": [
      "string"
    ],
    "RedactionOutput": "string",
    "RedactionType": "string"
  },
  "IdentifyLanguage": boolean,
  "IdentifyMultipleLanguages": boolean,
  "JobExecutionSettings": {
    "AllowDeferredExecution": boolean,
    "DataAccessRoleArn": "string"
  },
  "KMSEncryptionContext": {
    "string": "string"
  },
  "LanguageCode": "string",
  "LanguageIdSettings": {
    "string": {
      "LanguageModelName": "string",
      "VocabularyFilterName": "string",
      "VocabularyName": "string"
    }
  },
  "LanguageOptions": [
    "string"
  ],
  "Media": {
    "MediaFileUri": "string",
    "RedactedMediaFileUri": "string"
  },
  "MediaFormat": "string",
  "MediaSampleRateHertz": number,
  "ModelSettings": {
    "LanguageModelName": "string"
  },
  "OutputBucketName": "string",
  "OutputEncryptionKMSKeyId": "string",
  "OutputKey": "string",
  "Settings": {
    "ChannelIdentification": boolean,
    "MaxAlternatives": number,
    "MaxSpeakerLabels": number,
    "ShowAlternatives": boolean,
    "ShowSpeakerLabels": boolean,
    "VocabularyFilterMethod": "string",
    "VocabularyFilterName": "string",
    "VocabularyName": "string"
  },
  "Subtitles": {
    "Formats": [
      "string"
    ],
    "OutputStartIndex": number
  },
  "Tags": [
    {
      "Key": "string",
      "Value": "string"
    }
  ],
  "TranscriptionJobName": "string"
}
```