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
<div id="settings" class="hidden">
    <div id="settings-header-text" class="settings-header-text"><fmt:message key="partials.canvas.settings-content.settings-header-text"/></div>
    <div class="settings-container">
        <div>
            <div id="settings-tabs" class="settings-tabs tab-container"></div>
            <div class="clear"></div>
        </div>
        <div id="settings-tabs-content">
        	<fmt:message key="partials.canvas.settings-content.new-service-or-task" var="servicetask"/>
            <button id="new-service-or-task" class="add-button fa fa-plus" title="${servicetask}" style="display: block;"></button>
            <div id="general-settings-tab-content" class="configuration-tab">
                <div id="general-settings">
                    <div class="setting">
                        <div class="setting-name">
                            <fmt:message key="partials.canvas.settings-content.setting1"/>
                            <fmt:message key="partials.canvas.settings-content.setting1-title" var="setting1"/>
                            <div class="fa fa-question-circle" alt="Info" title="${setting1}"></div>
                        </div>
                        <div class="editable setting-field">
                            <input type="text" id="maximum-timer-driven-thread-count-field" class="setting-input"/>
                        </div>
                        <div class="read-only setting-field">
                            <span id="read-only-maximum-timer-driven-thread-count-field"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">
                            <fmt:message key="partials.canvas.settings-content.setting2"/>
                            <fmt:message key="partials.canvas.settings-content.setting2-title" var="setting2"/>
                            <div class="fa fa-question-circle" alt="Info" title="${setting2}"></div>
                        </div>
                        <div class="editable setting-field">
                            <input type="text" id="maximum-event-driven-thread-count-field" class="setting-input"/>
                        </div>
                        <div class="read-only setting-field">
                            <span id="read-only-maximum-event-driven-thread-count-field"></span>
                        </div>
                    </div>
                    <div class="editable settings-buttons">
                        <div id="settings-save" class="button"><fmt:message key="partials.canvas.settings-content.Apply"/></div>
                        <div class="clear"></div>
                    </div>
                </div>
            </div>
            <div id="controller-services-tab-content" class="configuration-tab controller-settings-table">
                <div id="controller-services-table" class="settings-table"></div>
            </div>
            <div id="reporting-tasks-tab-content" class="configuration-tab controller-settings-table">
                <div id="reporting-tasks-table" class="settings-table"></div>
            </div>
        </div>
    </div>
    <div id="settings-refresh-container">
    	<fmt:message key="partials.canvas.settings-content.Refresh" var="refresh"/>
        <button id="settings-refresh-button" class="refresh-button pointer fa fa-refresh" title="${refresh}"></button>
        <div id="settings-last-refreshed-container" class="last-refreshed-container">
            <fmt:message key="partials.canvas.settings-content.LastUpdated"/>:&nbsp;<span id="settings-last-refreshed" class="value-color"></span>
        </div>
        <div id="settings-loading-container" class="loading-container"></div>
        <div id="controller-cs-availability" class="hidden"><fmt:message key="partials.canvas.settings-content.container"/></div>
        <div class="clear"></div>
    </div>
</div>
