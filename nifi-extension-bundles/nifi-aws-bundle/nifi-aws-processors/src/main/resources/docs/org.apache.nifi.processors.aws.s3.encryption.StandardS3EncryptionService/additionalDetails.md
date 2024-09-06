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

# S3EncryptionService

## Description

The `StandardS3EncryptionService` manages an encryption strategy and applies that strategy to various S3 operations.  
**Note:** This service has no effect when a processor has the `Server Side Encryption` property set. To use this service
with processors so configured, first create a service instance, set the `Encryption Strategy` to `Server-side S3`,
disable the `Server Side Encryption` processor setting, and finally, associate the processor with the service.

## Configuration Details

### Encryption Strategy

The name of the specific encryption strategy for this service to use when encrypting and decrypting S3 operations.

* `None` - no encryption is configured or applied.
* `Server-side S3` - encryption and decryption is managed by S3; no keys are required.
* `Server-side KMS` - encryption and decryption are performed by S3 using the configured KMS key.
* `Server-side Customer Key` - encryption and decryption are performed by S3 using the supplied customer key.
* `Client-side KMS` - like the Server-side KMS strategy, with the encryption and decryption performed by the client.
* `Client-side Customer Key` - like the Server-side Customer Key strategy, with the encryption and decryption performed
  by the client.

### Key ID or Key Material

When configured for either the Server-side or Client-side KMS strategies, this field should contain the KMS Key ID.

When configured for either the Server-side or Client-side Customer Key strategies, this field should contain the key
material, and that material must be base64 encoded.

All other encryption strategies ignore this field.

### KMS Region

KMS key region, if any. This value must match the actual region of the KMS key if supplied.