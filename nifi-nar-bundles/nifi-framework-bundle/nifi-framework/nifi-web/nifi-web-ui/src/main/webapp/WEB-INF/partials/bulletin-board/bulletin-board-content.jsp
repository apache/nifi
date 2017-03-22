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
<div id="bulletin-board">
    <div id="bulletin-board-header-and-filter">
        <div id="bulletin-board-header-text"><fmt:message key="partials.bulletin-board.bulletin-board-content.bulletin-board-header-text"/></div>
        <div id="bulletin-board-filter-controls">
            <div id="bulletin-board-filter-container" class="filter-container">
            	<fmt:message key="partials.bulletin-board.bulletin-board-content.bulletin-board-filter" var="filter"/>
                <input type="text" placeholder="${filter}" id="bulletin-board-filter" class="filter"/>
                <div id="bulletin-board-filter-type" class="filter-type"></div>
            </div>
        </div>
    </div>
    <div id="bulletin-board-container"></div>
</div>
<div id="bulletin-board-refresh-container">
	<fmt:message key="partials.bulletin-board.bulletin-board-content.refresh-toggle" var="toggle"/>
    <md-switch class="md-primary refresh-toggle" aria-label="${toggle}" ng-change="appCtrl.serviceProvider.bulletinBoardCtrl.togglePolling()" ng-model="appCtrl.serviceProvider.bulletinBoardCtrl.polling">
        <fmt:message key="partials.bulletin-board.bulletin-board-content.bulletin-board-refresh-container"/>
    </md-switch>
    <fmt:message key="partials.bulletin-board.bulletin-board-content.bulletin-board-last-refreshed-container" var="autorefresh"/>
    <button id="refresh-button" ng-click="appCtrl.serviceProvider.bulletinBoardCtrl.loadBulletins()" class="refresh-button pointer fa fa-refresh" title="${autorefresh}"></button>
    <div id="bulletin-board-last-refreshed-container" class="last-refreshed-container">
        <fmt:message key="partials.bulletin-board.bulletin-board-content.bulletin-board-last-refreshed-container"/>:&nbsp;<span id="bulletin-board-last-refreshed" class="value-color"></span>
    </div>
    <div id="bulletin-board-loading-container" class="loading-container"></div>
    <div id="bulletin-board-status-container">
        <div id="bulletin-error-message"></div>
        <span id="clear-bulletins-button" class="link pointer"><fmt:message key="partials.bulletin-board.Clear"/></span>
    </div>
</div>