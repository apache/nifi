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
<div id="cluster-process-group-summary-dialog" class="hidden large-dialog">
    <div class="dialog-content">
        <div id="cluster-process-group-summary-header">
            <div id="cluster-process-group-details-container">
                <div id="cluster-process-group-icon"></div>
                <div id="cluster-process-group-details">
                    <div id="cluster-process-group-name"></div>
                    <div id="cluster-process-group-type"></div>
                </div>
                <div id="cluster-process-group-id"></div>
            </div>
        </div>
        <div id="cluster-process-group-summary-table"></div>
    </div>
    <button id="cluster-process-group-refresh-button" class="refresh-button pointer fa fa-refresh" title="Refresh"></button>
    <div id="cluster-process-group-summary-last-refreshed-container" class="last-refreshed-container">
        Last updated:&nbsp;<span id="cluster-process-group-summary-last-refreshed" class="value-color"></span>
    </div>
    <div id="cluster-process-group-summary-loading-container" class="loading-container"></div>
</div>
