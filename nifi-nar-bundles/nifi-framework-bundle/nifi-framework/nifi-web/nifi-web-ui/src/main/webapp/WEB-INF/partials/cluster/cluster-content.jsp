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
<div id="cluster">
    <div id="cluster-header-and-filter">
        <div id="cluster-header-text">NiFi Cluster</div>
        <div id="cluster-filter-controls">
            <div id="cluster-filter-stats" class="filter-status">
                Displaying&nbsp;<span id="displayed-nodes"></span>&nbsp;of&nbsp;<span id="total-nodes"></span>
            </div>
            <div id="cluster-filter-container" class="filter-container">
                <input type="text" id="cluster-filter" class="filter" placeholder="Filter"/>
                <div id="cluster-filter-type" class="filter-type"></div>
            </div>
        </div>
    </div>
    <div id="cluster-table"></div>
</div>
<div id="cluster-refresh-container">
    <button id="refresh-button" class="refresh-button pointer fa fa-refresh" title="Refresh"></button>
    <div id="cluster-last-refreshed-container" class="last-refreshed-container">
        Last updated:&nbsp;<span id="cluster-last-refreshed"></span>
    </div>
    <div id="cluster-loading-container" class="loading-container"></div>
</div>
