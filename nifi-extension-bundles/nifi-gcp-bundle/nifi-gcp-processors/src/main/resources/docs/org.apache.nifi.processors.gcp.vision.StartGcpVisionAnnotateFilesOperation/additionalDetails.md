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

# Google Vision

## Google Cloud Vision - Start Annotate Files Operation

Prerequisites

* Make sure Vision API is enabled and the account you are using has the right to use it
* Make sure the input file(s) are available in a GCS bucket

### Usage

StartGcpVisionAnnotateFilesOperation is designed to trigger file annotation operations. This processor should be used in
pair with the GetGcpVisionAnnotateFilesOperationStatus Processor. Outgoing FlowFiles contain the raw response to the
request returned by the Vision server. The response is in JSON format and contains the result and additional metadata as
written in the Google Vision API Reference documents.

### Payload

The JSON Payload is a request in JSON format as documented in
the [Google Vision REST API reference document](https://cloud.google.com/vision/docs/reference/rest/v1/files/asyncBatchAnnotate).
Payload can be fed to the processor via the `JSON Payload` property or as a FlowFile content. The property has higher
precedence over FlowFile content. Please make sure to delete the default value of the property if you want to use
FlowFile content payload. A JSON payload template example:

```json
{
  "requests": [
    {
      "inputConfig": {
        "gcsSource": {
          "uri": "gs://${gcs.bucket}/${filename}"
        },
        "mimeType": "application/pdf"
      },
      "features": [
        {
          "type": "${vision-feature-type}",
          "maxResults": 4
        }
      ],
      "outputConfig": {
        "gcsDestination": {
          "uri": "gs://${output-bucket}/${filename}/"
        },
        "batchSize": 2
      }
    }
  ]
}
```

### Features types

* TEXT\_DETECTION: Optical character recognition (OCR) for an image; text recognition and conversion to machine-coded
  text. Identifies and extracts UTF-8 text in an image.
* DOCUMENT\_TEXT\_DETECTION: Optical character recognition (OCR) for a file (PDF/TIFF) or dense text image; dense text
  recognition and conversion to machine-coded text.

You can find more details at [Google Vision Feature List](https://cloud.google.com/vision/docs/features-list)

### Example: How to set up a simple Annotate Image Flow

Prerequisites

* Create an input and output bucket
* Input files should be available in a GCS bucket
* This bucket must not contain anything else but the input files
* Set the bucket property of ListGCSBucket processor to your input bucket name
* Keep the default value of JSON PAYLOAD property in StartGcpVisionAnnotateFilesOperation
* Set the Output Bucket property to your output bucket name in StartGcpVisionAnnotateFilesOperation
* Setup GCP Credentials Provider Service for all GCP related processor

Execution steps:

* ListGCSBucket processor will return a list of files in the bucket at the first run.
* ListGCSBucket will return only new items at subsequent runs.
* StartGcpVisionAnnotateFilesOperation processor will trigger GCP Vision file annotation jobs based on the JSON payload.
* StartGcpVisionAnnotateFilesOperation processor will populate the `operationKey` flow file attribute.
* GetGcpVisionAnnotateFilesOperationStatus processor will periodically query status of the job.