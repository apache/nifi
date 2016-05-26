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
<div id="process-group-configuration">
    <div id="process-group-configuration-header-text" class="settings-header-text">Process Group Configuration</div>
    <div class="settings-container">
        <div class="settings-tabs-container">
            <div id="process-group-configuration-tabs" class="settings-tabs"></div>
            <div id="process-group-configuration-refresh-button" class="pointer settings-refresh-button" title="Refresh"></div>
            <div class="settings-last-refreshed-container">
                Last updated:&nbsp;<span id="process-group-configuration-last-refreshed"></span>
            </div>
            <div id="process-group-configuration-loading-container" class="loading-container"></div>
            <div id="add-process-group-configuration-controller-service" class="add-icon-bg"></div>
            <div class="clear"></div>
        </div>
        <div class="settings-tab-background"></div>
        <div>
            <div id="general-process-group-configuration-tab-content" class="configuration-tab">
                <div id="general-process-group-configuration">
                    <div class="setting">
                        <div class="setting-name">Process group name</div>
                        <div class="editable setting-field">
                            <input type="text" id="process-group-name" class="setting-input"/>
                        </div>
                        <div class="read-only setting-field">
                            <span id="read-only-process-group-name"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">Process group comments</div>
                        <div class="editable setting-field">
                            <textarea id="process-group-comments" class="setting-input"></textarea>
                        </div>
                        <div class="read-only setting-field">
                            <span id="read-only-process-group-comments"></span>
                        </div>
                    </div>
                    <div class="editable settings-buttons">
                        <div id="process-group-configuration-save" class="button">Apply</div>
                        <div class="clear"></div>
                    </div>
                </div>
            </div>
            <div id="process-group-controller-services-tab-content" class="configuration-tab">
                <div id="process-group-controller-services-table" class="settings-table"></div>
            </div>
        </div>
    </div>
</div>