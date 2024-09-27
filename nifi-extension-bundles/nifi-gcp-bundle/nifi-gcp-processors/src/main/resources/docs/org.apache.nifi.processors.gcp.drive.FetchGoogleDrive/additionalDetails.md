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

# FetchGoogleDrive

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
3. **Find File ID**  
   Usually FetchGoogleDrive is used with ListGoogleDrive and 'drive.id' is set.  
   In case 'drive.id' is not available, you can find the Drive ID of the file in the following way:
    * Right-click on the file and select "Get Link".
    * In the pop-up window click on "Copy Link".
    * You can obtain the file ID from the URL copied to clipboard. For example, if the URL were
      `https://drive.google.com/file/d/16ALV9KIU_KKeNG557zyctqy2Fmzyqtq/view?usp=share_link`,  
      the File ID would be `16ALV9KIU_KKeNG557zyctqy2Fmzyqtq`
4. **Set File ID in 'File ID' property**