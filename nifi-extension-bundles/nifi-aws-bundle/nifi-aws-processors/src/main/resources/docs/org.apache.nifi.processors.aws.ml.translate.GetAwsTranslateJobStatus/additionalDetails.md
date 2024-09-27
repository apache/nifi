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

## GetAwsTranslateJobStatus

Amazon Translate is a neural machine translation service for translating text to and from English across a breadth of
supported languages. Powered by deep-learning technologies, Amazon Translate delivers fast, high-quality, and affordable
language translation. It provides a managed, continually trained solution, so you can easily translate company and
user-authored content or build applications that require support across multiple languages. The machine translation
engine has been trained on a wide variety of content across different domains to produce quality translations that serve
any industry need.

### Usage

GetAwsTranslateJobStatus Processor is designed to periodically check translate job status. This processor should be used
in pair with Translate Processor. If the job successfully finished it will populate outputLocation attribute of the flow
file where you can find the output of the Translation job.