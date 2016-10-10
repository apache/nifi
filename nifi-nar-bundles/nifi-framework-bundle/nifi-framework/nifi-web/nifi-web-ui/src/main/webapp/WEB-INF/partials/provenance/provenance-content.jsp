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
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<div id="provenance">
    <span id="initial-component-query" class="hidden"><c:out value="${param.componentId}"/></span>
    <span id="initial-flowfile-query" class="hidden"><c:out value="${param.flowFileUuid}"/></span>
    <span id="nifi-controller-uri" class="hidden"></span>
    <span id="nifi-content-viewer-url" class="hidden"></span>
    <div id="provenance-header-text">NiFi Data Provenance</div>
    <div id="provenance-event-search" class="provenance-panel">
        <div id="provenance-filter-controls" class="filter-controls">
            <div id="provenance-filter-stats" class="filter-status">
                Displaying&nbsp;<span id="displayed-events"></span>&nbsp;of&nbsp;<span id="total-events"></span>
            </div>
            <div id="oldest-event-message">
                Oldest event available:&nbsp;<span id="oldest-event" class="value-color"></span>
            </div>
            <div id="provenance-filter-container" class="filter-container">
                <input type="text" placeholder="Filter" id="provenance-filter" class="filter"/>
                <div id="provenance-filter-type" class="filter-type"></div>
            </div>
        </div>
        <div id="provenance-search-container">
            <div id="provenance-search-overview">
                <span id="provenance-query-message">&nbsp;</span>
                <span id="clear-provenance-search" class="link">Clear search</span>
            </div>
            <button id="provenance-search-button" class="fa fa-search"></button>
        </div>
        <div id="provenance-table"></div>
        <div id="provenance-refresh-container">
            <button id="refresh-button" class="refresh-button pointer fa fa-refresh" title="Refresh"></button>
            <div id="provenance-last-refreshed-container" class="last-refreshed-container">
                Last updated:&nbsp;<span id="provenance-last-refreshed" class="value-color"></span>
            </div>
            <div id="provenance-loading-container" class="loading-container"></div>
        </div>
    </div>
    <div id="provenance-lineage" class="provenance-panel hidden">
        <div id="provenance-lineage-loading-container">
            <div id="provenance-lineage-loading" class="loading-container"></div>
        </div>
        <div id="provenance-lineage-close-container">
            <div id="provenance-lineage-downloader" class="fa fa-download" title="Download"></div>
            <div id="provenance-lineage-closer" class="fa fa-long-arrow-left" title="Go back to event list"></div>
        </div>
        <div id="provenance-lineage-context-menu" class="context-menu"></div>
        <div id="provenance-lineage-slider-container">
            <div id="provenance-lineage-slider"></div>
            <div id="event-timeline">
                <div id="event-time"></div>
                <span style="float: left;">&nbsp;</span>
                <div id="event-timezone" class="timezone"></div>
                <div id="clear"></div>
            </div>
        </div>
        <div id="provenance-lineage-container"></div>
    </div>
    <a id="image-download-link" hreflang="image/svg+xml" class="hidden" target="_blank"></a>
</div>