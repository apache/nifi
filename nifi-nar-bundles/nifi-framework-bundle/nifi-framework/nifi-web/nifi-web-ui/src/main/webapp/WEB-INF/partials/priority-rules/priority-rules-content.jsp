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
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<div id="priority-rules">
    <div id="priority-rules-header-and-filter">
        <div id="priority-rules-header-text">NiFi Priority Rules</div>
        <div id="priority-rules-filter-controls">
            <div id="priority-rules-filter-stats" class="filter-status">
                Displaying&nbsp;<span id="displayed-priority-rules"></span>&nbsp;of&nbsp;<span id="total-priority-rules"></span>
            </div>
            <div id="priority-rules-filter-container" class="filter-container">
                <input type="text" id="priority-rules-filter" class="filter" placeholder="Filter"/>
                <div id="priority-rules-filter-type" class="filter-type"></div>
                <div id="priority-rules-controls-container">
                    <button id="new-priority-rule-button" class="fa fa-user-plus priority-button" title="Add priority rule"></button>
                </div>
            </div>
        </div>
    </div>
    <div id="priority-rules-table"></div>
</div>
<div id="add-priority-rule-dialog">
    <div class="secure-port-setting">
        <input id="add-priority-rule-label-field" type="text" placeholder="Label"/>
    </div>
    <div class="secure-port-setting">
        <input id="add-priority-rule-expression-field" type="text" placeholder="Expression"/>
    </div>
    <div class="secure=port-setting">
        <input id="add-priority-rule-rate-field" type="text" placeholder="Priority Value (Interpreted as a percentage)"/>
    </div>
</div>
<div id="edit-priority-rule-dialog">
    <div class="secure-port-setting">
        <input id="edit-priority-rule-uuid-field" type="hidden"/>
    </div>
    <div class="secure-port-setting">
        <input id="edit-priority-rule-label-field" type="text" placeholder="Label"/>
    </div>
    <div class="secure-port-setting">
        <input id="edit-priority-rule-expression-field" type="text" placeholder="Expression"/>
    </div>
    <div class="secure-port-setting">
        <input id="edit-priority-rule-rate-field" type="text" placeholder="Priority Value (Interpreted as a percentage"/>
    </div>
</div>
<div id="priority-rules-refresh-container">