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
<div id="variable-registry-dialog" class="hidden">
    <div class="dialog-content">
        <div class="settings-left">
            <div class="setting">
                <div style="float: left;">
                    <div class="setting-name">Process Group</div>
                    <div class="setting-field">
                        <span id="process-group-variable-registry"></span>
                        <span id="variable-registry-process-group-id" class="hidden"></span>
                    </div>
                </div>
                <div id="add-variable"><button class="button fa fa-plus"></button></div>
                <div class="clear"></div>
            </div>
            <div id="variable-registry-table"></div>
            <div id="variable-update-status" class="hidden">
                <div class="setting">
                    <div class="setting-name">
                        Steps to update variables
                    </div>
                    <div class="setting-field">
                        <ol id="variable-update-steps"></ol>
                    </div>
                </div>
            </div>
        </div>
        <div class="spacer">&nbsp;</div>
        <div class="settings-right">
            <div class="setting">
                <div class="setting-name">
                    Variables
                </div>
                <div class="setting-field">
                    <div id="affected-components-context"></div>
                </div>
            </div>
            <div class="setting">
                <div class="setting-name">
                    Referencing Processors
                    <div class="fa fa-question-circle" alt="Info" title="Processors referencing this variable."></div>
                </div>
                <div class="setting-field">
                    <ul id="variable-registry-affected-processors"></ul>
                </div>
            </div>
            <div class="setting">
                <div class="setting-name">
                    Referencing Controller Services
                    <div class="fa fa-question-circle" alt="Info" title="Controller Services referencing this variable."></div>
                </div>
                <div class="setting-field">
                    <ul id="variable-registry-affected-controller-services"></ul>
                </div>
            </div>
            <div class="setting">
                <div class="setting-name">
                    Unauthorized referencing components
                    <div class="fa fa-question-circle" alt="Info" title="Referencing components for which READ or WRITE permissions are not granted."></div>
                </div>
                <div class="setting-field">
                    <ul id="variable-registry-affected-unauthorized-components"></ul>
                </div>
            </div>
        </div>
    </div>
    <div id="variable-message">Variables do not support sensitive values and will be included when versioning a Process Group.</div>
</div>
<div id="new-variable-dialog" class="dialog cancellable small-dialog hidden">
    <div class="dialog-content">
        <div>
            <div class="setting-name">Variable name</div>
            <div class="setting-field new-variable-name-container">
                <input id="new-variable-name" type="text"/>
            </div>
        </div>
    </div>
</div>