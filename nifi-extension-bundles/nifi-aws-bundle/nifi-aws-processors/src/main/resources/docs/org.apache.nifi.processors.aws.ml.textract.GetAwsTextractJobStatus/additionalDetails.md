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

# Amazon Textract

## GetAwsTextractJobStatus

Amazon Textract is a machine learning (ML) service that automatically extracts text, handwriting, and data from scanned
documents. It goes beyond simple optical character recognition (OCR) to identify, understand, and extract data from
forms and tables.

### Usage

GetAwsTextractJobStatus Processor is designed to periodically check textract job status. This processor should be used
in pair with StartAwsTextractJob Processor. FlowFile will contain the serialized Tetract response that contains the
result and additional metadata as it is documented in AWS Textract Reference.