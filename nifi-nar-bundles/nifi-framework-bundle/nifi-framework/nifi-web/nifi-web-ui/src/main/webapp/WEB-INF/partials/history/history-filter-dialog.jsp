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
<div id="history-filter-dialog" class="hidden medium-dialog">
    <div class="dialog-content">
        <div class="setting">
            <div class="setting-name"><fmt:message key="partials.history.history-filter-dialog.Filter"/></div>
            <div class="setting-field">
                <div id="history-filter-controls">
                    <input type="text" id="history-filter" class="history-large-input"/>
                    <div id="history-filter-type"></div>
                    <div class="clear"></div>
                </div>
            </div>
        </div>
        <div class="setting">
            <div class="start-date-setting">
                <div class="setting-name">
                    <fmt:message key="partials.history.history-filter-dialog.StartDate"/>
                    <fmt:message key="partials.history.history-filter-dialog.StartDate.title" var="history-filter-dialog_StartDate"/>
                    <div class="fa fa-question-circle" alt="Info" title="${history-filter-dialog_StartDate}"></div>
                </div>
                <div class="setting-field">
                    <input type="text" id="history-filter-start-date" class="history-small-input"/>
                </div>
            </div>
            <div class="start-time-setting">
                <div class="setting-name">
                    <fmt:message key="partials.history.history-filter-dialog.StartTime"/> (<span class="timezone"></span>)
                    <fmt:message key="partials.history.history-filter-dialog.StartTime.title" var="history-filter-dialog_StartTime"/>
                    <div class="fa fa-question-circle" alt="Info" title="${history-filter-dialog_StartTime}"></div>
                </div>
                <div class="setting-field">
                    <input type="text" id="history-filter-start-time" class="history-small-input"/>
                </div>
            </div>
            <div class="clear"></div>
        </div>
        <div class="setting">
            <div class="end-date-setting">
                <div class="setting-name">
                    <fmt:message key="partials.history.history-filter-dialog.EndDate"/>
                    <fmt:message key="partials.history.history-filter-dialog.EndDate.title" var="history-filter-dialog_EndDate"/>
                    <div class="fa fa-question-circle" alt="Info" title="${history-filter-dialog_EndDate}"></div>
                </div>
                <div class="setting-field">
                    <input type="text" id="history-filter-end-date" class="history-small-input"/>
                </div>
            </div>
            <div class="end-time-setting">
                <div class="setting-name">
                    <fmt:message key="partials.history.history-filter-dialog.EndTime"/> (<span class="timezone"></span>)
                    <fmt:message key="partials.history.history-filter-dialog.EndTime.title" var="history-filter-dialog_EndTime"/>
                    <div class="fa fa-question-circle" alt="Info" title="${history-filter-dialog_EndTime}"></div>
                </div>
                <div class="setting-field">
                    <input type="text" id="history-filter-end-time" class="history-small-input"/>
                </div>
            </div>
            <div class="clear"></div>
        </div>
    </div>
</div>
