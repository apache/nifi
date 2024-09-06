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

# PutS3Object

## Multi-part Upload Details

The upload uses either the PutS3Object method or the PutS3MultipartUpload method. The PutS3Object method sends the file
in a single synchronous call, but it has a 5GB size limit. Larger files are sent using the PutS3MultipartUpload method.
This multipart process saves state after each step so that a large upload can be resumed with minimal loss if the
processor or cluster is stopped and restarted. A multipart upload consists of three steps:

1. Initiate upload
2. Upload the parts
3. Complete the upload

For multipart uploads, the processor saves state locally tracking the upload ID and parts uploaded, which must both be
provided to complete the upload. The AWS libraries select an endpoint URL based on the AWS region, but this can be
overridden with the 'Endpoint Override URL' property for use with other S3-compatible endpoints. The S3 API specifies
that the maximum file size for a PutS3Object upload is 5GB. It also requires that parts in a multipart upload must be at
least 5MB in size, except for the last part. These limits establish the bounds for the Multipart Upload Threshold and
Part Size properties.

## Configuration Details

### Object Key

The Object Key property value should not start with "/".

### Credentials File

The Credentials File property allows the user to specify the path to a file containing the AWS access key and secret
key. The contents of the file should be in the following format:

```
    [default]
    accessKey=<access key>
    secretKey=<security key>
```

Make sure the credentials file is readable by the NiFi service user.

When using the Credential File property, ensure that there are no values for the Access Key and Secret Key properties.
The Value column should read "No value set" for both. **Note:** Do not check "Set empty string" for either as the empty
string is considered a set value.