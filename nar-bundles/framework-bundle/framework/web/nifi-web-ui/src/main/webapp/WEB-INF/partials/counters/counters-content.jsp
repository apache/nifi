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
            <div id="counters-filter-container">
                <input type="text" id="counters-filter"/>
                <div id="counters-filter-type"></div>
            </div>
            <div id="counters-filter-stats">
                Displaying&nbsp;<span id="displayed-counters"></span>&nbsp;of&nbsp;<span id="total-counters"></span>
            </div>
        </div>
    </div>
    <div id="counters-refresh-container">
        <div id="refresh-button" class="counters-refresh pointer" title="Refresh"></div>
        <div id="counters-last-refreshed-container">
            Last updated:&nbsp;<span id="counters-last-refreshed"></span>
        </div>
        <div id="counters-loading-container" class="loading-container"></div>
    </div>
    <div id="counters-table"></div>
</div>