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

## GetAwsPollyJobStatus

Amazon Polly is a service that turns text into lifelike speech, allowing you to create applications that talk, and build
entirely new categories of speech-enabled products. Polly's Text-to-Speech (TTS) service uses advanced deep learning
technologies to synthesize natural sounding human speech. With dozens of lifelike voices across a broad set of
languages, you can build speech-enabled applications that work in many different countries.

### Usage

GetAwsPollyJobStatus Processor is designed to periodically check polly job status. This processor should be used in pair
with StartAwsPollyJob Processor. If the job successfully finished it will populate `outputLocation` attribute of the
flow file where you can find the output of the Polly job. In case of an error `failure.reason` attribute will be
populated with the details.