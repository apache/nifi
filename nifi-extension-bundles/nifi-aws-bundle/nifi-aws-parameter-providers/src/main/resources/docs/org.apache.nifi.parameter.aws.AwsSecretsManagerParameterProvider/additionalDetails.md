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

# AWSSecretsManagerParameterProvider

### Mapping AWS Secrets to Parameter Contexts

The AwsSecretsManagerParameterProvider maps a Secret to a Parameter Context, with key/value pairs in the Secret mapping
to parameters. To create a compatible secret from the AWS Console:

1. From the Secrets Manager service, click the "Store a new Secret" button
2. Select "Other type of secret"
3. Under "Key/value", enter your parameters, with the parameter names being the keys and the parameter values being the
   values. Click Next.
4. Enter the Secret name. This will determine which Parameter Context receives the parameters. Continue through the rest
   of the wizard and finally click the "Store" button.

Alternatively, from the command line, run a command like the following:

aws secretsmanager create-secret --name "\[Context\]" --secret-string '{ "\[Param\]": "\[secretValue\]", "\[Param2\]": "
\[secretValue2\]" }'

In this example, \[Context\] should be the intended name of the Parameter Context, \[Param\] and \[Param2\] should be
parameter names, and \[secretValue\] and \[secretValue2\] should be the values of each respective parameter.

### Configuring the Parameter Provider

AWS Secrets must be explicitly matched in the "Secret Name Pattern" property in order for them to be fetched. This
prevents more than the intended Secrets from being pulled into NiFi.