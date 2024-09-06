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

# Amazon Polly

## StartAwsPollyJob

Amazon Polly is a service that turns text into lifelike speech, allowing you to create applications that talk, and build
entirely new categories of speech-enabled products. Polly's Text-to-Speech (TTS) service uses advanced deep learning
technologies to synthesize natural sounding human speech. With dozens of lifelike voices across a broad set of
languages, you can build speech-enabled applications that work in many different countries.

### Usage

Amazon ML Processors are implemented to utilize ML services based on the official AWS API Reference. You can find
example json payload in the documentation at the Request Syntax sections. For more details please check the
official [Polly API reference](https://docs.aws.amazon.com/polly/latest/dg/API_Reference.html) With this processor you
will trigger a startSpeechSynthesisTask async call to Polly Service. You can define json payload as property or provide
as a flow file content. Property has higher precedence. After the job is triggered the serialized json response will be
written to the output flow file. The `awsTaskId` attribute will be populated, so it makes it easier to query job status
by the corresponding get job status processor.

JSON payload template - note that it can be simplified with the optional fields,
check [AWS documentation for more details](https://docs.aws.amazon.com/polly/latest/dg/API_StartSpeechSynthesisTask.html) -
example:

```json
{
  "Engine": "string",
  "LanguageCode": "string",
  "LexiconNames": [
    "string"
  ],
  "OutputFormat": "string",
  "OutputS3BucketName": "string",
  "OutputS3KeyPrefix": "string",
  "SampleRate": "string",
  "SnsTopicArn": "string",
  "SpeechMarkTypes": [
    "string"
  ],
  "Text": "string",
  "TextType": "string",
  "VoiceId": "string"
}
```