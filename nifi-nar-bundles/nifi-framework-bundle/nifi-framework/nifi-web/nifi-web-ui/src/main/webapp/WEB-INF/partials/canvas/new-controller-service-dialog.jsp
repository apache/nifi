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
<div id="new-controller-service-dialog" layout="column" class="hidden large-dialog">
    <div class="dialog-content">
        <div layout="row" style="padding-bottom:0">
            <div flex="25" layout="row" layout-align="start center">
                <div class="setting-name"><fmt:message key="partials.canvas.new-controller-service-dialog.TagCloud"/>:</div>
            </div>
            <div flex layout="row" layout-align="space-between center" id="controller-service-type-filter-controls" class="filter-status">
                <div id="controller-service-type-filter-status">
                    <fmt:message key="partials.canvas.new-controller-service-dialog.Displaying"/>&nbsp;<span id="displayed-controller-service-types"></span>&nbsp;<fmt:message key="partials.canvas.new-controller-service-dialog.Of"/>&nbsp;<span id="total-controller-service-types"></span>
                </div>
                <div id="controller-service-type-filter-container">
                	<fmt:message key="partials.canvas.new-controller-service-dialog.controller-service-type-filter" var="filter"/>
                    <input type="text" id="controller-service-type-filter" placeholder="${filter}"/>
                </div>
            </div>
        </div>
        <div flex layout="row" style="padding-top:0;height: 90%;">
            <div flex="25" id="controller-service-tag-cloud-container">
                <div class="setting">
                    <div class="setting-field">
                        <div id="controller-service-tag-cloud"></div>
                    </div>
                </div>
            </div>
            <div layout="column" flex id="controller-service-types-container">
                <div id="controller-service-types-table" class="unselectable"></div>
                <div id="controller-service-type-name-title" class="ellipsis"><fmt:message key="partials.canvas.new-controller-service-dialog.controller-service-type-name-title"/>:</div>
                <div id="controller-service-type-name" class="ellipsis"></div>
                <div id="controller-service-description-container" class="hidden">
                    <div id="controller-service-type-description" class="ellipsis multiline"></div>
                    <span class="hidden" id="selected-controller-service-name"></span>
                    <span class="hidden" id="selected-controller-service-type"></span>
                </div>
            </div>
        </div>
        <div class="clear"></div>
    </div>
</div>
