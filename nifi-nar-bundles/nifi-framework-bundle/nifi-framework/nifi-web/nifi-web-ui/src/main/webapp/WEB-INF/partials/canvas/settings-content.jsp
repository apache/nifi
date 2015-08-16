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
    <div id="settings-header-text">NiFi Settings</div>
    <div id="settings-container">
        <div id="settings-tabs-container">
            <div id="settings-tabs"></div>
            <div id="settings-refresh-button" class="pointer" title="Refresh"></div>
            <div id="settings-last-refreshed-container">
                Last updated:&nbsp;<span id="settings-last-refreshed"></span>
            </div>
            <div id="settings-refresh-required-icon" class="hidden"></div>
            <div id="settings-loading-container" class="loading-container"></div>
            <div id="new-service-or-task" class="add-icon-bg"></div>
            <div class="clear"></div>
        </div>
        <div id="settings-tab-background"></div>
        <div id="settings-tabs-content">
            <div id="general-settings-tab-content" class="configuration-tab">
                <div id="general-settings">
                    <div class="setting">
                        <div class="setting-name">Data flow name</div>
                        <div class="editable setting-field">
                            <input type="text" id="data-flow-title-field" name="data-flow-title" class="setting-input"/>
                            <span id="archive-flow-link" class="link">Back-up flow</span>
                            <img class="setting-icon icon-info" src="images/iconInfo.png" alt="Info" title="Archives the flow configuration."/>
                        </div>
                        <div class="read-only setting-field">
                            <span id="read-only-data-flow-title-field"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">Data flow comments</div>
                        <div class="editable setting-field">
                            <textarea id="data-flow-comments-field" name="data-flow-comments" class="setting-input"></textarea>
                        </div>
                        <div class="read-only setting-field">
                            <span id="read-only-data-flow-comments-field"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">
                            Maximum timer driven thread count
                            <img class="setting-icon icon-info" src="images/iconInfo.png" alt="Info" title="The maximum number of threads for timer driven processors available to the system."/>
                        </div>
                        <div class="editable setting-field">
                            <input type="text" id="maximum-timer-driven-thread-count-field" class="setting-input"/>
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
                    <div id="settings-buttons" class="editable">
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