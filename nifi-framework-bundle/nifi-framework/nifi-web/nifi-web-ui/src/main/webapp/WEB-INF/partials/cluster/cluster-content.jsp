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
    </div>
    <div id="cluster-tabs" class="tab-container"></div>
    <div id="cluster-tabs-content">
        <div id="cluster-filter-controls" class="filter-controls">
            <div id="cluster-filter-stats" class="filter-status">
                Displaying&nbsp;<span id="displayed-rows"></span>&nbsp;of&nbsp;<span id="total-rows"></span>
            </div>
            <div id="cluster-filter-container" class="filter-container">
                <input type="text" id="cluster-filter" class="filter" placeholder="Filter"/>
                <div id="cluster-filter-type" class="filter-type"></div>
            </div>
        </div>
        <div id="cluster-nodes-tab-content" class="configuration-tab">
            <div id="cluster-nodes-table" class="cluster-tabbed-table"></div>
        </div>
        <div id="cluster-system-tab-content" class="configuration-tab">
            <div id="cluster-system-table" class="cluster-tabbed-table"></div>
        </div>
        <div id="cluster-jvm-tab-content" class="configuration-tab">
            <div id="cluster-jvm-table" class="cluster-tabbed-table"></div>
        </div>
        <div id="cluster-flowfile-tab-content" class="configuration-tab">
            <div id="cluster-flowfile-table" class="cluster-tabbed-table"></div>
        </div>
        <div id="cluster-content-tab-content" class="configuration-tab">
            <div id="cluster-content-table" class="cluster-tabbed-table"></div>
        </div>
        <div id="cluster-provenance-tab-content" class="configuration-tab">
            <div id="cluster-provenance-table" class="cluster-tabbed-table"></div>
        </div>
        <div id="cluster-version-tab-content" class="configuration-tab">
            <div id="cluster-version-table" class="cluster-tabbed-table"></div>
        </div>
    </div>
</div>
<div id="cluster-refresh-container">
    <button id="refresh-button" class="refresh-button pointer fa fa-refresh" title="Refresh"></button>
    <div id="cluster-last-refreshed-container" class="last-refreshed-container">
        Last updated:&nbsp;<span id="cluster-last-refreshed"></span>
    </div>
    <div id="cluster-loading-container" class="loading-container"></div>
</div>
