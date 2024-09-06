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

# ConsumeIMAP

## Description:

This Processor consumes email messages via IMAP protocol and sends the content of an email message as content of the
Flow File. Content of the incoming email message is written as raw bytes to the content of the outgoing Flow File.

Different email providers may require additional Java Mail properties which could be provided as dynamic properties. For
example, below is a sample configuration for GMail:

**Processor's static properties:**

* **Host Name** - imap.gmail.com
* **Port** - 993
* **User Name** - _\[your user name\]_
* **Password** - _\[your password\]_
* **Folder** - INBOX

**Processor's dynamic properties:**

* **mail.imap.socketFactory.class** - javax.net.ssl.SSLSocketFactory
* **mail.imap.socketFactory.fallback** - false
* **mail.store.protocol** - imaps

Another useful property is **mail.debug** which allows Java Mail API to print protocol messages to the console helping
you to both understand what's going on and debug issues.

For the full list of available Java Mail properties please refer
to [here](https://javaee.github.io/javamail/docs/api/com/sun/mail/imap/package-summary.html)