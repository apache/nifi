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
<div id="status-history-dialog" class="hidden large-dialog">
    <div id="status-history-refresh-container">
        <button id="status-history-refresh-button" class="refresh-button pointer fa fa-refresh" title="Refresh"></button>
        <div id="status-history-last-refreshed-container" class="last-refreshed-container">
            Last updated:&nbsp;<span id="status-history-last-refreshed"></span>
        </div>
        <div id="status-history-loading-container" class="loading-container"></div>
    </div>
    <div class="dialog-content">
        <div id="status-history-details"></div>
        <div id="status-history-metric-combo"></div>
        <div id="status-history-container">
            <div id="status-history-chart-container"></div>
            <div id="status-history-chart-control-container"></div>
        </div>
        <div class="clear"></div>
    </div>
</div>
