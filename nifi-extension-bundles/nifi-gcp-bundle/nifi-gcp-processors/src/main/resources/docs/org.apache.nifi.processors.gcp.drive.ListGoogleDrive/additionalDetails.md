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

# ListGoogleDrive

## Accessing Google Drive from NiFi

This processor uses Google Cloud credentials for authentication to access Google Drive. The following steps are required
to prepare the Google Cloud and Google Drive accounts for the processors:

1. **Enable Google Drive API in Google Cloud**
    * Follow instructions
      at [https://developers.google.com/workspace/guides/enable-apis](https://developers.google.com/workspace/guides/enable-apis)
      and search for 'Google Drive API'.
2. **Grant access to Google Drive folder**
    * In Google Cloud Console navigate to IAM & Admin -> Service Accounts.
    * Take a note of the email of the service account you are going to use.
    * Navigate to the folder to be listed in Google Drive.
    * Right-click on the Folder -> Share.
    * Enter the service account email.
3. **Find Folder ID**
    * Navigate to the folder to be listed in Google Drive and enter it. The URL in your browser will include the ID at
      the end of the URL. For example, if the URL were
      `https://drive.google.com/drive/folders/1trTraPVCnX5_TNwO8d9P_bz278xWOmGm`, the Folder ID would be
      `1trTraPVCnX5_TNwO8d9P_bz278xWOmGm`
4. **Set Folder ID in 'Folder ID' property**