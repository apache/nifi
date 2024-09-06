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

# ExternalHazelcastCacheManager

This service connects to an external Hazelcast cluster (or standalone instance) as client. Hazelcast 4.0.0 or newer
version is required. The connection to the server is kept alive using Hazelcast's built-in reconnection capability. This
might be fine-tuned by setting the following properties:

* Hazelcast Initial Backoff
* Hazelcast Maximum Backoff
* Hazelcast Backoff Multiplier
* Hazelcast Connection Timeout

If the service cannot connect or abruptly disconnected it tries to reconnect after a backoff time. The amount of time
waiting before the first attempt is defined by the Initial Backoff. If the connection is still not successful the client
waits gradually more between the attempts until the waiting time reaches the value set in the 'Hazelcast Maximum
Backoff' property (or the connection timeout, whichever is smaller). The backoff time after the first attempt is always
based on the previous amount, multiplied by the Backoff Multiplier. Note: the real backoff time might be slightly differ
as some "jitter" is added to the calculation in order to avoid regularity.