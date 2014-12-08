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
        <div id="general-settings">
            <div class="setting">
                <div class="setting-name">Data flow name</div>
                <div class="setting-field">
                    <input type="text" id="data-flow-title-field" name="data-flow-title" class="setting-input"/>
                    <span id="archive-flow-link" class="link">Back-up flow</span>
                    <img class="setting-icon icon-info" src="images/iconInfo.png" alt="Info" title="Archives the flow configuration."/>
                </div>
            </div>
            <div class="setting">
                <div class="setting-name">Data flow comments</div>
                <div class="setting-field">
                    <textarea id="data-flow-comments-field" name="data-flow-comments" class="setting-input"></textarea>
                </div>
            </div>
            <div class="setting">
                <div class="setting-name">
                    Maximum timer driven thread count
                    <img class="setting-icon icon-info" src="images/iconInfo.png" alt="Info" title="The maximum number of threads for timer driven processors available to the system."/>
                </div>
                <div class="setting-field">
                    <input type="text" id="maximum-timer-driven-thread-count-field" class="setting-input"/>
                </div>
            </div>
            <div class="setting">
                <div class="setting-name">
                    Maximum event driven thread count
                    <img class="setting-icon icon-info" src="images/iconInfo.png" alt="Info" title="The maximum number of threads for event driven processors available to the system."/>
                </div>
                <div class="setting-field">
                    <input type="text" id="maximum-event-driven-thread-count-field" class="setting-input"/>
                </div>
            </div>
        </div>
        <div id="settings-buttons">
            <div id="settings-cancel" class="button">Cancel</div>
            <div id="settings-save" class="button">Apply</div>
        </div>
    </div>
</div>