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
<div id="bulletin-board">
    <div id="bulletin-board-header-and-filter">
        <div id="bulletin-board-header-text">NiFi Templates</div>
        <div id="bulletin-board-filter-controls">
            <div id="bulletin-board-filter-container" class="filter-container">
                <input type="text" placeholder="Filter" id="bulletin-board-filter" class="filter"/>
                <div id="bulletin-board-filter-type" class="filter-type"></div>
            </div>
        </div>
    </div>
    <div id="bulletin-board-container"></div>
</div>
<div id="bulletin-board-refresh-container">
    <md-switch class="md-primary refresh-toggle" aria-label="Toggle auto refresh" ng-change="appCtrl.serviceProvider.bulletinBoardCtrl.togglePolling()" ng-model="appCtrl.serviceProvider.bulletinBoardCtrl.polling">
        Auto-refresh
    </md-switch>
    <button id="refresh-button" ng-click="appCtrl.serviceProvider.bulletinBoardCtrl.loadBulletins()" class="refresh-button pointer fa fa-refresh" title="Start/Stop auto refresh"></button>
    <div id="bulletin-board-last-refreshed-container" class="last-refreshed-container">
        Last updated:&nbsp;<span id="bulletin-board-last-refreshed" class="value-color"></span>
    </div>
    <div id="bulletin-board-loading-container" class="loading-container"></div>
    <div id="bulletin-board-status-container">
        <div id="bulletin-error-message"></div>
        <span id="clear-bulletins-button" class="link pointer">Clear</span>
    </div>
</div>