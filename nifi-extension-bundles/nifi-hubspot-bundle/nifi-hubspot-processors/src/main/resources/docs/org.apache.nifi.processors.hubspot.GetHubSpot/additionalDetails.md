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

# GetHubSpot

## Authentication Methods

The processor is working with HubSpot private applications. A HubSpot private app must be created (
see [HubSpot Private App Creation](https://developers.hubspot.com/docs/api/private-apps)) in order to connect to HubSpot
and make requests. Private App Access Tokens are the only authentication method that is currently supported.

## Incremental Loading

HubSpot objects can be processed incrementally by NiFi. This means that only objects created or modified after the last
run time of the processor are processed. The processor state can be reset in the context menu. The incremental loading
is based on the objects last modified time.

## Paging

GetHubSpot supports both paging and incrementality at the same time. In case the number of results exceeds the 'Result
Limit', in the next processor run the remaining objects will be returned.

Due to the page handling mechanism of the HubSpot API, parallel deletions are not supported. Some objects may be omitted
if any object is deleted between fetching two pages.