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
<div id="import-flow-version-dialog" layout="column" class="hidden large-dialog">
    <div class="dialog-content">
        <div class="setting">
            <div class="setting-name">Registry</div>
            <div class="setting-field">
                <div id="import-flow-version-registry-combo"></div>
                <div id="import-flow-version-registry" class="hidden"></div>
            </div>
        </div>
        <div class="setting">
            <div class="setting-name">Bucket</div>
            <div class="setting-field">
                <div id="import-flow-version-bucket-combo"></div>
                <div id="import-flow-version-bucket" class="hidden"></div>
            </div>
        </div>
        <div class="setting">
            <div class="setting-name">Name</div>
            <div class="setting-field">
                <span id="import-flow-version-process-group-id" class="hidden"></span>
                <div id="import-flow-version-name-combo"></div>
                <div id="import-flow-version-name" class="hidden"></div>
            </div>
        </div>
        <div id="import-flow-version-container" class="setting hidden">
            <div class="setting-name">Current Version</div>
            <div class="setting-field">
                <div id="import-flow-version-label"></div>
            </div>
        </div>
        <div id="import-flow-version-table"></div>
    </div>
</div>
<div id="change-version-status-dialog" layout="column" class="hidden small-dialog">
    <div class="dialog-content">
        <div class="setting">
            <div class="setting-field">
                <div id="change-version-status-message"></div>
            </div>
            <div class="setting-field">
                <div id="change-version-percent-complete"></div>
            </div>
        </div>
    </div>
</div>