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
<div id="queue-listing-container" class="hidden" class="hidden medium-dialog">
    <div id="queue-listing-header-container">
        <div id="queue-listing-header-text"></div>
    </div>
    <div id="queue-listing-stats-container">
        <div id="queue-listing-stats" class="filter-status">
            <fmt:message key="partials.canvas.queue-listing.queue-listing-stats-deploy"/>&nbsp;<span id="displayed-flowfiles"></span>&nbsp;<fmt:message key="partials.canvas.queue-listing.queue-listing-stats-of"/>&nbsp;<span id="total-flowfiles-count"></span>&nbsp;(<span id="total-flowfiles-size"></span>)
        </div>
        <div class="clear"></div>
    </div>
    <div id="queue-listing-table"></div>
    <div id="queue-listing-message" class="hidden"></div>
    <div id="queue-listing-refresh-container">
    	<fmt:message key="partials.canvas.queue-listing.queue-listing-refresh-button" var="refresh"/>
        <button id="queue-listing-refresh-button" class="refresh-button pointer fa fa-refresh" title="${refresh}"></button>
        <div id="queue-listing-last-refreshed-container" class="last-refreshed-container">
            <fmt:message key="partials.canvas.queue-listing.lastUpdate"/>:&nbsp;<span id="queue-listing-last-refreshed"></span>
        </div>
        <div id="queue-listing-loading-container" class="loading-container"></div>
    </div>
</div>
