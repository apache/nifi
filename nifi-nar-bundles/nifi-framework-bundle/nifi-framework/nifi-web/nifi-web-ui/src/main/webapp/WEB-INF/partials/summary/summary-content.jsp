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
<div id="summary">
    <div id="summary-header-and-filter">
        <div id="summary-header-text"><fmt:message key="partials.summary.summary-content.summary-header-text"/></div>
    </div>
    <div id="summary-tabs" class="tab-container"></div>
    <div id="summary-tabs-content">
        <div id="summary-filter-controls" class="filter-controls">
            <div id="summary-filter-status" class="filter-status">
                <fmt:message key="partials.summary.summary-content.Displaying"/>&nbsp;<span id="displayed-items"></span>&nbsp;<fmt:message key="partials.summary.summary-content.Of"/>&nbsp;<span id="total-items"></span>
            </div>
            <fmt:message key="partials.summary.summary-content.summary-filter-container" var="filter"/>
            <div id="summary-filter-container" class="filter-container">
                <input type="text" placeholder="${filter}" id="summary-filter" class="filter"/>
                <div id="summary-filter-type" class="filter-type"></div>
            </div>
        </div>
        <div id="view-options-container">
            <fmt:message key="partials.summary.summary-content.View"/>&nbsp;
            <span id="view-single-node-link" class="link"><fmt:message key="partials.summary.summary-content.SingleNode"/></span>&nbsp;&nbsp;<span id="view-cluster-link" class="link"><fmt:message key="partials.summary.summary-content.Cluster"/></span>
        </div>
        <div id="processor-summary-tab-content" class="configuration-tab">
            <div id="processor-summary-table" class="summary-table"></div>
        </div>
        <div id="connection-summary-tab-content" class="configuration-tab">
            <div id="connection-summary-table" class="summary-table"></div>
        </div>
        <div id="process-group-summary-tab-content" class="configuration-tab">
            <div id="process-group-summary-table" class="summary-table"></div>
        </div>
        <div id="input-port-summary-tab-content" class="configuration-tab">
            <div id="input-port-summary-table" class="summary-table"></div>
        </div>
        <div id="output-port-summary-tab-content" class="configuration-tab">
            <div id="output-port-summary-table" class="summary-table"></div>
        </div>
        <div id="remote-process-group-summary-tab-content" class="configuration-tab">
            <div id="remote-process-group-summary-table" class="summary-table"></div>
        </div>
    </div>
</div>
<div id="flow-summary-refresh-container">
	<fmt:message key="partials.summary.summary-content.Refresh" var="refresh"/>
    <button id="refresh-button" class="refresh-button pointer fa fa-refresh" title="${refresh}"></button>
    <div id="summary-last-refreshed-container" class="last-refreshed-container">
        <fmt:message key="partials.summary.summary-content.LastUpdated"/>&nbsp;<span id="summary-last-refreshed" class="value-color"></span>
    </div>
    <div id="summary-loading-container" class="loading-container"></div>
    <div id="system-diagnostics-link-container">
        <span id="system-diagnostics-link" class="link"><fmt:message key="partials.summary.summary-content.SystemDiagnostics"/></span>
    </div>
</div>