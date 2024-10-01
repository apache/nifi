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

# PutEmail

# OAuth Authorization Mode

PutEmail can use OAuth2. The exact way may depend on the email provider.


## OAuth with Gmail

### Configure Gmail OAuth Client

The Gmail OAuth client can be used to send email on behalf of multiple different gmail accounts so this needs to be done once.

1.  In the Google Development Console [Create a project](https://support.google.com/googleapi/answer/6251787) (if you don't have one yet)
2.  [Configure OAuth consent](https://console.cloud.google.com/apis/credentials/consent)
3.  [Create OAuth client](https://console.cloud.google.com/apis/credentials/oauthclient). Select **Desktop app** as **Application type**. When the client has been created, take note of the Client ID and Client secret values as they will be needed later.

### Retrieve Token for NiFi

Tokens are provided once the owner of the Gmail account consented to the previously created client to send emails on their behalf. Consequently, this needs to be done for every gmail account.

1.  Go to the following web page:

    https://accounts.google.com/o/oauth2/auth?redirect\_uri=urn%3Aietf%3Awg%3Aoauth%3A2.0%3Aoob&response\_type=code&scope=https%3A%2F%2Fmail.google.com&client\_id=_CLIENT\_ID_

    Replace CLIENT\_ID at the end to your Client ID.
2.  You may need to select the Google Account for which you want to consent. Click **Continue** twice.
3.  A page will appear with an Authorisation code that will have a message at the bottom like this:

    **Authorisation code**

    Please copy this code, switch to your application and paste it there:

    _AUTHORISATION\_CODE_


4.  Execute the following command from terminal to fetch the access and refresh tokens.  
    In case the curl command returns an error, please try again from step 1.

    curl https://accounts.google.com/o/oauth2/token -d grant\_type=authorization\_code -d redirect\_uri="urn:ietf:wg:oauth:2.0:oob" -d client\_id=_CLIENT\_ID_ -d client\_secret=_CLIENT\_SECRET_ -d code=_AUTHORISATION\_CODE_

    Replace CLIENT\_ID, CLIENT\_SECRET and AUTHORISATION\_CODE to your values.
5.  The curl command results a json file which contains the access token and refresh token:

```json
{
  "access_token": "ACCESS_TOKEN",
  "expires_in": 3599,
  "refresh_token": "REFRESH_TOKEN",
  "scope": "https://mail.google.com/",
  "token_type": "Bearer"
}
```



### Configure Token in NiFi

1.  On the PutEmail processor in the **Authorization Mode** property select **Use OAuth2**.
2.  In the **OAuth2 Access Token Provider** property select/create a StandardOauth2AccessTokenProvider controller service.
3.  On the StandardOauth2AccessTokenProvider controller service in the **Grant Type** property select **Refresh Token**.
4.  In the **Refresh Token** property enter the REFRESH\_TOKEN returned by the curl command.
5.  In the **Authorization Server URL** enter

    https://accounts.google.com/o/oauth2/token

6.  Also fill in the **Client ID** and **Client secret** properties.