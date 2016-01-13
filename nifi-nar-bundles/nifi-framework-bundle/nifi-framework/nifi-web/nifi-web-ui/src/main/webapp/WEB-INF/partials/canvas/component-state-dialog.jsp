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
<div id="component-state-dialog">
    <div class="dialog-content">
        <div class="setting">
            <div class="setting-name">Name</div>
            <div class="setting-field">
                <span id="component-state-name"></span>
            </div>
        </div>
        <div class="setting">
            <div class="setting-name">Description</div>
            <div class="setting-field">
                <span id="component-state-description"></span>
            </div>
        </div>
        <div id="component-state-filter-controls">
            <div id="component-state-filter-container">
                <input type="text" id="component-state-filter"/>
            </div>
            <div id="component-state-filter-status">
                Displaying&nbsp;<span id="displayed-component-state-entries"></span>&nbsp;of&nbsp;<span id="total-component-state-entries"></span>
            </div>
        </div>
        <div id="component-state-table"></div>
        <div id="clear-link-container">
            <span id="clear-link" class="link">Clear state</span>
        </div>
    </div>
</div>