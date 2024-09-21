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

## Google Cloud Vision - Start Annotate Images Operation

Prerequisites

* Make sure Vision API is enabled and the account you are using has the right to use it
* Make sure the input image(s) are available in a GCS bucket under `/input` folder

### Usage

StartGcpVisionAnnotateImagesOperation is designed to trigger image annotation operations. This processor should be used
in pair with the GetGcpVisionAnnotateImagesOperationStatus Processor. Outgoing FlowFiles contain the raw response to the
request returned by the Vision server. The response is in JSON format and contains the result and additional metadata as
written in the Google Vision API Reference documents.

### Payload

The JSON Payload is a request in JSON format as documented in
the [Google Vision REST API reference document](https://cloud.google.com/vision/docs/reference/rest/v1/images/asyncBatchAnnotate).
Payload can be fed to the processor via the `JSON Payload` property or as a FlowFile content. The property has higher
precedence over FlowFile content. Please make sure to delete the default value of the property if you want to use
FlowFile content payload. A JSON payload template example:

```json
{
  "requests": [
    {
      "image": {
        "source": {
          "imageUri": "gs://${gcs.bucket}/${filename}"
        }
      },
      "features": [
        {
          "type": "${vision-feature-type}",
          "maxResults": 4
        }
      ]
    }
  ],
  "outputConfig": {
    "gcsDestination": {
      "uri": "gs://${output-bucket}/${filename}/"
    },
    "batchSize": 2
  }
}
```

### Features types

* TEXT\_DETECTION: Optical character recognition (OCR) for an image; text recognition and conversion to machine-coded
  text. Identifies and extracts UTF-8 text in an image.
* DOCUMENT\_TEXT\_DETECTION: Optical character recognition (OCR) for a file (PDF/TIFF) or dense text image; dense text
  recognition and conversion to machine-coded text.
* LANDMARK\_DETECTION: Provides the name of the landmark, a confidence score and a bounding box in the image for the
  landmark.
* LOGO\_DETECTION: Provides a textual description of the entity identified, a confidence score, and a bounding polygon
  for the logo in the file.
* LABEL\_DETECTION: Provides generalized labels for an image.
* etc.

You can find more details at [Google Vision Feature List](https://cloud.google.com/vision/docs/features-list)

### Example: How to set up a simple Annotate Image Flow

Prerequisites

* Create an input and output bucket
* Input image files should be available in a GCS bucket
* This bucket must not contain anything else but the input image files
* Set the bucket property of ListGCSBucket processor to your input bucket name
* Keep the default value of JSON PAYLOAD property in StartGcpVisionAnnotateImagesOperation
* Set the Output Bucket property to your output bucket name in StartGcpVisionAnnotateImagesOperation
* Setup GCP Credentials Provider Service for all GCP related processor

Execution steps:

* ListGCSBucket processor will return a list of files in the bucket at the first run.
* ListGCSBucket will return only new items at subsequent runs.
* StartGcpVisionAnnotateImagesOperation processor will trigger GCP Vision image annotation jobs based on the JSON
  payload.
* StartGcpVisionAnnotateImagesOperation processor will populate the `operationKey` flow file attribute.
* GetGcpVisionAnnotateImagesOperationStatus processor will periodically query status of the job.