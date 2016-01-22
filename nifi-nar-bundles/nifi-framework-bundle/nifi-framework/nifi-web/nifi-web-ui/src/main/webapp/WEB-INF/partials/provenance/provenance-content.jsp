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
    <span id="intial-component-query" class="hidden"><c:out value="${param.componentId}"/></span>
    <span id="intial-flowfile-query" class="hidden"><c:out value="${param.flowFileUuid}"/></span>
    <span id="nifi-controller-uri" class="hidden"></span>
    <span id="nifi-content-viewer-url" class="hidden"></span>
    <div id="provenance-header-and-filter">
        <div id="provenance-header-text">NiFi Data Provenance</div>
        <div id="provenance-filter-controls">
            <div id="provenance-filter-container">
                <input type="text" id="provenance-filter"/>
                <div id="provenance-filter-type"></div>
            </div>
            <div id="provenance-filter-stats">
                Displaying&nbsp;<span id="displayed-events"></span>&nbsp;of&nbsp;<span id="total-events"></span>
            </div>
        </div>
        <div id="oldest-event-message">
            Oldest event available:&nbsp;<span id="oldest-event"></span>
        </div>
    </div>
    <div id="provenance-event-search" class="provenance-panel">
        <div id="provenance-refresh-container">
            <div id="refresh-button" class="history-refresh pointer" title="Refresh"></div>
            <div id="provenance-last-refreshed-container">
                Last updated:&nbsp;<span id="provenance-last-refreshed"></span>
            </div>
            <div id="provenance-loading-container" class="loading-container"></div>
            <div id="provenance-search-container">
                <div id="provenance-search-overview">
                    <span id="provenance-query-message"></span>
                    <span id="clear-provenance-search">Clear search</span>
                </div>
                <div id="provenance-search-button" class="button button-normal pointer">Search</div>
            </div>
        </div>
        <div id="provenance-table"></div>
    </div>
    <div id="provenance-lineage" class="provenance-panel hidden">
        <div id="provenance-lineage-loading-container">
            <div id="provenance-lineage-loading" class="loading-container"></div>
        </div>
        <div id="provenance-lineage-close-container">
            <div id="provenance-lineage-downloader" title="Download"></div>
            <div id="provenance-lineage-closer" title="Close"></div>
        </div>
        <div id="provenance-lineage-context-menu"></div>
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
</div>