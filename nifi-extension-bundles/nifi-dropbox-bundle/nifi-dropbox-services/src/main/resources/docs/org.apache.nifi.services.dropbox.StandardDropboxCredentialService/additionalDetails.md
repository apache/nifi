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

# StandardDropboxCredentialService

## Generating credentials for Dropbox authentication

StandardDropboxCredentialService requires "App Key", "App Secret", "Access Token" and "Refresh Token".

This document describes how to generate these credentials using an existing Dropbox account.

### Generate App Key and App Secret

* Login with your Dropbox account.
* If you already have an app created, go to [Dropbox Developers](https://www.dropbox.com/developers) page, click on "App
  Console" button and select your app. On the app's info page you will find the "App key" and "App secret".  
  (See also [Dropbox Getting Started](https://www.dropbox.com/developers/reference/getting-started), App Console tab, "
  Navigating the App Console" chapter)
* If you don't have any apps, go to [Dropbox Developers](https://www.dropbox.com/developers) page and click on "Create
  app" button. (See also [Dropbox Getting Started](https://www.dropbox.com/developers/reference/getting-started), App
  Console tab, "Creating a Dropbox app" chapter.)
    * On the next page select "Scoped access" and "Full Dropbox" as access type.
    * Provide a name for your app.
    * On the app's info page you will find the "App key" and "App secret". (See
      also [Dropbox Getting Started](https://www.dropbox.com/developers/reference/getting-started), App Console tab, "
      Navigating the App Console" chapter.)

### Set required permissions for your app

The "files.content.read" permission has to be enabled for the application to be able to read the files in Dropbox.

You can set permissions in [Dropbox Developers](https://www.dropbox.com/developers) page.

* Click on "App Console" button and select your app.
* Go to "Permissions" tab and enable the "files.content.read" permission.
* Click "Submit" button.
* NOTE: In case you already have an Access Token and Refresh Token, those tokens have to be regenerated after the
  permission change. See "Generate Access Token and Refresh Token" chapter about token generation.

### Generate Access Token and Refresh Token

* Go to the following web page:

  https://www.dropbox.com/oauth2/authorize?token\_access\_type=offline&response\_type=code&client\_id=_your\_app\_key_

* Click "Next" and click on "Allow" button on the next page.
* An access code will be generated for you, it will be displayed on the next page:

  "Access Code Generated
  Enter this code into your\_app\_name to finish the process
  _your\_generated\_access\_code_"


* Execute the following command from terminal to fetch the access and refresh tokens.

  Make sure you execute the curl command right after the access code generation, since the code expires very quickly.  
  In case the curl command returns "invalid grant" error, please generate a new access code (see previous step)

  curl https://api.dropbox.com/oauth2/token -d code=_your\_generated\_access\_code_ -d grant\_type=authorization\_code
  -u _your\_app\_key_:_your\_app\_secret_


* The curl command results a json file which contains the "access\_token" and "refresh\_token":

```json
{
  "access_token": "sl.xxxxxxxxxxx"
  "expires_in": 14400,
  "refresh_token": "xxxxxx",
  "scope": "files.content.read files.metadata.read",
  "uid": "xxxxxx",
  "account_id": "dbid:xxxx"
}
```