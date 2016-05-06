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
<div id="flow-status" flex layout="row" layout-align="space-between center">
    <div id="flow-status-container" layout="row" layout-align="space-around center">
        <i class="fa fa-cubes" ng-if="appCtrl.nf.Canvas.isClustered()"><span id="connected-nodes-count"></span></i>
        <i class="icon icon-threads"><span id="active-thread-count"></span></i>
        <i class="fa fa-list"><span id="total-queued"></span></i>
        <i class="fa fa-bullseye"><span id="controller-transmitting-count"></span></i>
        <i class="icon icon-transmit-false"><span id="controller-not-transmitting-count"></span></i>
        <i class="fa fa-play"><span id="controller-running-count"></span></i>
        <i class="fa fa-stop"><span id="controller-stopped-count"></span></i>
        <i class="fa fa-warning"><span id="controller-invalid-count"></span></i>
        <i class="icon icon-enable-false"><span id="controller-disabled-count"></span></i>
        <i class="fa fa-refresh"><span id="stats-last-refreshed"></span></i>
        <div id="canvas-loading-container" class="loading-container"></div>
    </div>
    <div layout="row" layout-align="end center">
        <input id="search-field" type="text"/>
        <button id="search-button"><i class="fa fa-search"></i></button>
        <button id="bulletin-button"><i class="fa fa-sticky-note-o"></i></button>
    </div>
</div>
<div id="search-flow-results"></div>
