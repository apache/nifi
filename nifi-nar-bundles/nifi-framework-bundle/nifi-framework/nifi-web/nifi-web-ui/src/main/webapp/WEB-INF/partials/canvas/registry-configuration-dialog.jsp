<%--
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
--%>
<%@ page contentType="text/html" pageEncoding="UTF-8" session="false" %>
<div id="registry-configuration-dialog" layout="column" class="hidden medium-dialog">
    <div class="dialog-content">
        <div class="setting">
            <div class="setting-name">Name</div>
            <div class="setting-field">
                <span id="registry-id" class="hidden"></span>
                <input type="text" id="registry-name" class="setting-input"/>
            </div>
        </div>
        <div class="setting">
            <div class="setting-name">URL</div>
            <div class="setting-field">
                <input type="text" id="registry-location" class="setting-input" placeholder="https://remotehost:8443"/>
            </div>
        </div>
        <div class="setting">
            <div class="setting-name">Description</div>
            <div class="setting-field">
                <textarea id="registry-description" class="setting-input"></textarea>
            </div>
        </div>
    </div>
</div>
