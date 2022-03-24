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
<div id="remote-process-group-ports" class="hidden large-dialog">
    <div class="dialog-content">
        <span id="remote-process-group-ports-id" class="hidden"></span>
        <div class="settings-left">
            <div class="setting">
                <div class="setting-name">Name</div>
                <div class="setting-field">
                    <span id="remote-process-group-ports-name"></span>
                </div>
            </div>
            <div class="remote-port-header">
                <div>Input ports</div>
            </div>
            <div id="remote-process-group-input-ports-container" class="remote-ports-container"></div>
        </div>
        <div class="spacer">&nbsp;</div>
        <div class="settings-right">
            <div class="setting">
                <div class="setting-name">URLs</div>
                <div class="setting-field">
                    <span id="remote-process-group-ports-urls"></span>
                </div>
            </div>
            <div class="remote-port-header">
                <div>Output ports</div>
            </div>
            <div id="remote-process-group-output-ports-container" class="remote-ports-container"></div>
        </div>
    </div>
</div>