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

# FetchBoxFile

## Fetch Box files in NiFi

1. **Find File ID**  
   Usually FetchBoxFile is used with ListBoxFile and 'box.id' is set.

   In case 'box.id' is not available, you can find the ID of the file in the following way:
    * Click on the file.
    * The URL in the browser will include the File ID.  
      For example, if the URL were `https://app.box.com/file/1012106094023?s=ldiqjwuor2vwdxeeap2rtcz66dql89h3`,  
      the File ID would be `1012106094023`
2. **Set File ID in 'File ID' property**