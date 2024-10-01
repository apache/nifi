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

# KubernetesSecretParameterProvider

### Deriving Parameters from mounted Kubernetes Secret files

The KubernetesSecretParameterProvider maps a directory to a parameter group named after the directory, and the files
within the directory to parameters. Each file's name is mapped to a parameter, and the content of the file becomes the
value. Hidden files and nested directories are ignored.

While this provider can be useful in a range of cases since it simply reads parameter values from local files, it
particularly matches the mounted volume secret structure in Kubernetes. A full discussion of Kubernetes secrets is
beyond the scope of this document, but a brief overview can illustrate how these secrets can be mapped to parameter
groups.

### Kubernetes Mounted Secrets Example

Assume a secret is configured as follows:

```yml
data:
  admin_username: my-username (base64-encoded)
  admin_password: my-password (base64-encoded)
  access_key: my-key (base64-encoded)
```

Assume a deployment has the following configuration:

```yml
spec:
  volumes:
  - name: system-credentials
    secret:
    items:
      - key: admin_username
        path: sys.admin.username
      - key: admin_password
        path: sys.admin.password
      - key: access_key
        path: sys.access.key
        secretName: system-creds
        containers:
  - volumeMounts:
      - mountPath: /etc/secrets/system-credentials
        name: system-credentials
        readOnly: true
```

Then, this secret will appear on disk as follows:

```
$ ls /etc/secrets/system-credentials
sys.access.key sys.admin.password sys.admin.username
```

Therefore, to map this secret to a parameter group that will populate a Parameter Context named 'system-credentials',
you should simply provide the following configuration to the KubernetesSecretParameterProvider:

* **Parameter Group Directories** - /etc/secrets/system-credentials

The 'system-credentials' parameter context will then contain the following parameters:

* **sys.access.key** - my-key
* **sys.admin.username** - my-username
* **sys.admin.password** - my-password