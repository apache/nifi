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
<div id="new-reporting-task-dialog" layout="column" class="hidden">
    <div class="dialog-content">
        <div layout="row" style="padding-bottom:0">
            <div flex="25" layout="row" layout-align="start center">
                <div class="setting-name" style="margin-top: 10px;">Source</div>
            </div>
            <div flex layout="row" layout-align="space-between center" id="reporting-task-type-filter-controls" class="filter-status">
                <div id="reporting-task-type-filter-status">
                    Displaying&nbsp;<span id="displayed-reporting-task-types"></span>&nbsp;of&nbsp;<span id="total-reporting-task-types"></span>
                </div>
                <div id="controller-service-type-filter-container">
                    <input type="text" id="reporting-task-type-filter" placeholder="Filter"/>
                </div>
            </div>
        </div>
        <div flex layout="row" style="padding-top:0;height: 90%;">
            <div flex="25" id="reporting-task-tag-cloud-container">
                <div class="setting">
                    <div class="setting-field">
                        <div id="reporting-task-bundle-group-combo"></div>
                    </div>
                </div>
                <div class="setting">
                    <div class="setting-field">
                        <div id="reporting-task-tag-cloud"></div>
                    </div>
                </div>
            </div>
            <div layout="column" flex id="reporting-task-types-container">
                <div id="reporting-task-types-table" class="unselectable"></div>
                <div id="reporting-task-type-container">
                    <div id="reporting-task-type-name"></div>
                    <div id="reporting-task-type-bundle"></div>
                </div>
                <div id="reporting-task-description-container" class="hidden">
                    <div id="reporting-task-type-description" class="ellipsis multiline"></div>
                    <span class="hidden" id="selected-reporting-task-name"></span>
                    <span class="hidden" id="selected-reporting-task-type"></span>
                </div>
            </div>
        </div>
        <div class="clear"></div>
    </div>
</div>
