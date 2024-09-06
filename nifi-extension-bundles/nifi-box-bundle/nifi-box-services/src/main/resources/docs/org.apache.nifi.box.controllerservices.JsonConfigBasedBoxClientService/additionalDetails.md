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

# JsonConfigBasedBoxClientService

## Setting up a Box App

This processor requires a pre-configured App under the Account owning the resources being
accessed (https://app.box.com/developers/console).

The App should have the following configuration:

* If you create a new App, select 'Server Authentication (with JWT)' authentication method.  
  If you want to use an existing App, choose one with 'OAuth 2.0 with JSON Web Tokens (Server Authentication)' as
  Authentication method.
* Should have a 'Client ID' and 'Client Secret'.
* 'App Access Level' should be 'App + Enterprise Access'.
* 'Application Scopes' should have 'Write all files and folders in Box' enabled.
* 'Advanced Features' should have 'Generate user access tokens' and 'Make API calls using the as-user header' enabled.
* Under 'Add and Manage Public Keys' generate a Public/Private Keypair and download the configuration JSON file (under
  App Settings). The full path of this file should be set in the 'Box Config File' property.  
  Note that you can only download the configuration JSON with the keypair details only once, when you generate the
  keypair. Also this is the only time Box will show you the private key.  
  If you want to download the configuration JSON file later (under 'App Settings') - or if you want to use your own
  keypair - after you download it you need to edit the file and add the keypair details manually.
* After all settings are done, the App needs to be reauthorized on the admin page. ('Reauthorize App'
  at https://app.box.com/master/custom-apps.)  
  If the app is configured for the first time it needs to be added ('Add App') before it can be authorized.