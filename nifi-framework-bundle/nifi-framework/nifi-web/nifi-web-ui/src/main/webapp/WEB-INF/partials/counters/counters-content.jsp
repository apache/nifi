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
<div id="counters">
    <div id="counters-header-and-filter">
        <div id="counters-header-text">NiFi Counters</div>
        <div id="counters-filter-controls">
            <div id="counters-filter-stats" class="filter-status">
                Displaying&nbsp;<span id="displayed-counters"></span>&nbsp;of&nbsp;<span id="total-counters"></span>
            </div>
            <div id="counters-filter-container" class="filter-container">
                <input type="text" id="counters-filter" placeholder="Filter" class="filter"/>
                <div id="counters-filter-type" class="filter-type"></div>
            </div>
        </div>
    </div>
    <div id="counters-table"></div>
</div>
<div id="counters-refresh-container">
    <button id="refresh-button" class="refresh-button pointer fa fa-refresh" title="Refresh"></button>
    <div id="counters-last-refreshed-container" class="last-refreshed-container">
        Last updated:&nbsp;<span id="counters-last-refreshed" class="value-color"></span>
    </div>
    <div id="counters-loading-container" class="loading-container"></div>
</div>