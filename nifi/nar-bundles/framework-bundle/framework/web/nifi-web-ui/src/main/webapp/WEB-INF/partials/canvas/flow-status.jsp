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
<div id="flow-status">
    <div class="flow-status-property-element">
        Active threads:
        <span id="active-thread-count" class="flow-status-property-value"></span>
    </div>
    <div class="flow-status-property-element">
        Queued:
        <span id="total-queued" class="flow-status-property-value"></span>
    </div>
    <div id="connected-nodes-element" class="flow-status-property-element">
        Connected nodes:
        <span id="connected-nodes-count" class="flow-status-property-value"></span>
    </div>
    <div class="flow-status-property-element">
        Stats last refreshed:
        <span id="stats-last-refreshed" class="flow-status-property-value"></span>
    </div>
    <div id="refresh-required-container" class="flow-status-property-element">
        <div id="refresh-required-icon"></div>
        <span id="refresh-required-link" class="link">Refresh</span>
    </div>
    <div id="controller-bulletins" class="bulletin-icon"></div>
    <div id="canvas-loading-container" class="loading-container"></div>
    <div id="controller-counts">
        <div class="transmitting"></div>
        <div id="controller-transmitting-count" class="controller-component-count">0</div>
        <div class="not-transmitting"></div>
        <div id="controller-not-transmitting-count" class="controller-component-count">0</div>
        <div class="running"></div>
        <div id="controller-running-count" class="controller-component-count">-</div>
        <div class="stopped"></div>
        <div id="controller-stopped-count" class="controller-component-count">-</div>
        <div class="invalid"></div>
        <div id="controller-invalid-count" class="controller-component-count">-</div>
        <div class="disabled"></div>
        <div id="controller-disabled-count" class="controller-component-count">-</div>
    </div>
</div>
