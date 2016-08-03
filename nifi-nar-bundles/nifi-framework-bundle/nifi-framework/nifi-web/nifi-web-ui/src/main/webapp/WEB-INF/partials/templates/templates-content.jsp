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
<div id="templates">
    <span id="template-group-id" class="hidden"><c:out value="${param.groupId}"/></span>
    <div id="templates-header-and-filter">
        <div id="templates-header-text">NiFi Templates</div>
        <div id="templates-filter-controls">
            <div id="templates-filter-stats" class="filter-status">
                Displaying&nbsp;<span id="displayed-templates"></span>&nbsp;of&nbsp;<span id="total-templates"></span>
            </div>
            <div id="templates-filter-container" class="filter-container">
                <input type="text" id="templates-filter" class="filter" placeholder="Filter"/>
                <div id="templates-filter-type" class="filter-type"></div>
            </div>
        </div>
    </div>
    <div id="templates-table"></div>
</div>
<div id="templates-refresh-container">
    <button id="refresh-button" class="refresh-button pointer fa fa-refresh" title="Refresh"></button>
    <div id="templates-last-refreshed-container" class="last-refreshed-container">
        Last updated:&nbsp;<span id="templates-last-refreshed" class="value-color"></span>
    </div>
    <div id="templates-loading-container" class="loading-container"></div>
</div>