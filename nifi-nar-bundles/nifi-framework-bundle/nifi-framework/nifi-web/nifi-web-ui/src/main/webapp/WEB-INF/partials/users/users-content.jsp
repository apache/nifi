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
<%@ taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt" %>
<div id="users">
    <div id="users-header-and-filter">
        <div id="users-header-text"><fmt:message key="partials.users.users-content.users-header-text"/></div>
    </div>
    <div id="users-filter-controls">
        <div id="users-filter-status" class="filter-status">
            <fmt:message key="partials.users.users-content.Displaying"/>&nbsp;<span id="displayed-users"></span>&nbsp;<fmt:message key="partials.users.users-content.Of"/>&nbsp;<span id="total-users"></span>
        </div>
        <div id="users-filter-container">
        <fmt:message key="partials.users.users-content.users-filter-container" var="filter"/>
            <input type="text" placeholder="${filter}" id="users-filter" class="filter"/>
            <div id="users-filter-type" class="filter-type"></div>
        </div>
        <button id="new-user-button" class="fa fa-user-plus"></button>
        <div class="clear"></div>
    </div>
    <div id="users-table"></div>
</div>
<div id="users-refresh-container">
	<fmt:message key="partials.users.users-content.Refresh" var="refresh"/>
    <button id="user-refresh-button" class="refresh-button pointer fa fa-refresh" title="${refresh}"></button>
    <div id="users-last-refreshed-container" class="last-refreshed-container">
        <fmt:message key="partials.users.users-content.LastUpdated"/>:&nbsp;<span id="users-last-refreshed" class="value-color"></span>
    </div>
    <div id="users-loading-container" class="loading-container"></div>
</div>