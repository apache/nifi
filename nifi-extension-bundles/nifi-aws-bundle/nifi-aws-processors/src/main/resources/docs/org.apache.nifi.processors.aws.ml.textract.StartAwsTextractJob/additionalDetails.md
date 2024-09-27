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

## StartAwsTextractJob

Amazon Textract is a machine learning (ML) service that automatically extracts text, handwriting, and data from scanned
documents. It goes beyond simple optical character recognition (OCR) to identify, understand, and extract data from
forms and tables.

### Usage

Amazon ML Processors are implemented to utilize ML services based on the official AWS API Reference. You can find
example json payload in the documentation at the Request Syntax sections. For more details please check the
official [Textract API reference](https://docs.aws.amazon.com/textract/latest/dg/API_Reference.html) With this processor
you will trigger a startDocumentAnalysis, startDocumentTextDetection or startExpenseAnalysis async call according to
your type of textract settings. You can define json payload as property or provide as a flow file content. Property has
higher precedence. After the job is triggered the serialized json response will be written to the output flow file. The
awsTaskId attribute will be populated, so it makes it easier to query job status by the corresponding get job status
processor.

Three different type of textract task are supported: Documnet Analysis, Text Detection, Expense Analysis.

### DocumentAnalysis

Starts the asynchronous analysis of an input document for relationships between detected items such as key-value pairs,
tables, and selection
elements. [API Reference](https://docs.aws.amazon.com/textract/latest/dg/API_StartDocumentAnalysis.html)

Example payload:

```json
{
  "ClientRequestToken": "string",
  "DocumentLocation": {
    "S3Object": {
      "Bucket": "string",
      "Name": "string",
      "Version": "string"
    }
  },
  "FeatureTypes": [
    "string"
  ],
  "JobTag": "string",
  "KMSKeyId": "string",
  "NotificationChannel": {
    "RoleArn": "string",
    "SNSTopicArn": "string"
  },
  "OutputConfig": {
    "S3Bucket": "string",
    "S3Prefix": "string"
  },
  "QueriesConfig": {
    "Queries": [
      {
        "Alias": "string",
        "Pages": [
          "string"
        ],
        "Text": "string"
      }
    ]
  }
}
```

### ExpenseAnalysis

Starts the asynchronous analysis of invoices or receipts for data like contact information, items purchased, and vendor
names. [API Reference](https://docs.aws.amazon.com/textract/latest/dg/API_StartExpenseAnalysis.html)

Example payload:

```json
{
  "ClientRequestToken": "string",
  "DocumentLocation": {
    "S3Object": {
      "Bucket": "string",
      "Name": "string",
      "Version": "string"
    }
  },
  "JobTag": "string",
  "KMSKeyId": "string",
  "NotificationChannel": {
    "RoleArn": "string",
    "SNSTopicArn": "string"
  },
  "OutputConfig": {
    "S3Bucket": "string",
    "S3Prefix": "string"
  }
}
```

### StartDocumentTextDetection

Starts the asynchronous detection of text in a document. Amazon Textract can detect lines of text and the words that
make up a line of
text. [API Reference](https://docs.aws.amazon.com/textract/latest/dg/API_StartDocumentTextDetection.html)

Example payload:

```json
{
  "ClientRequestToken": "string",
  "DocumentLocation": {
    "S3Object": {
      "Bucket": "string",
      "Name": "string",
      "Version": "string"
    }
  },
  "JobTag": "string",
  "KMSKeyId": "string",
  "NotificationChannel": {
    "RoleArn": "string",
    "SNSTopicArn": "string"
  },
  "OutputConfig": {
    "S3Bucket": "string",
    "S3Prefix": "string"
  }
}
```