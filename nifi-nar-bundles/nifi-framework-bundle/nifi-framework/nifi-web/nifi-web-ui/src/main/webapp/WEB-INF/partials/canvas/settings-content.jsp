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
<div id="settings">
    <div id="settings-header-text" class="settings-header-text">NiFi Settings</div>
    <div class="settings-container">
        <div class="settings-tabs-container">
            <div id="settings-tabs" class="settings-tabs"></div>
            <div id="settings-refresh-button" class="pointer settings-refresh-button" title="Refresh"></div>
            <div class="settings-last-refreshed-container">
                Last updated:&nbsp;<span id="settings-last-refreshed"></span>
            </div>
            <div id="settings-loading-container" class="loading-container"></div>
            <div id="new-service-or-task" class="add-icon-bg"></div>
            <div class="clear"></div>
        </div>
        <div class="settings-tab-background"></div>
        <div>
            <div id="general-settings-tab-content" class="configuration-tab">
                <div id="general-settings">
                    <div class="setting">
                        <div class="setting-name">
                            Maximum timer driven thread count
                            <img class="setting-icon icon-info" src="images/iconInfo.png" alt="Info" title="The maximum number of threads for timer driven processors available to the system."/>
                        </div>
                        <div class="editable setting-field">
                            <input type="text" id="maximum-timer-driven-thread-count-field" class="setting-input"/>
                            <span id="archive-flow-link" class="link">Back-up flow</span>
                            <img class="setting-icon icon-info" src="images/iconInfo.png" alt="Info" title="Archives the flow configuration."/>
                        </div>
                        <div class="read-only setting-field">
                            <span id="read-only-maximum-timer-driven-thread-count-field"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">
                            Maximum event driven thread count
                            <img class="setting-icon icon-info" src="images/iconInfo.png" alt="Info" title="The maximum number of threads for event driven processors available to the system."/>
                        </div>
                        <div class="editable setting-field">
                            <input type="text" id="maximum-event-driven-thread-count-field" class="setting-input"/>
                        </div>
                        <div class="read-only setting-field">
                            <span id="read-only-maximum-event-driven-thread-count-field"></span>
                        </div>
                    </div>
                    <div class="editable settings-buttons">
                        <div id="settings-save" class="button">Apply</div>
                        <div class="clear"></div>
                    </div>
                </div>
            </div>
            <div id="controller-services-tab-content" class="configuration-tab">
                <div id="controller-services-table" class="settings-table"></div>
            </div>
            <div id="reporting-tasks-tab-content" class="configuration-tab">
                <div id="reporting-tasks-table" class="settings-table"></div>
            </div>
        </div>
    </div>
</div>