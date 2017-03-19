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
<div id="provenance-search-dialog" class="hidden medium-dialog">
    <div class="dialog-content">
        <div class="setting">
            <div class="setting-name"><fmt:message key="partials.provenance.provenance-search-dialog.Fields"/></div>
            <div class="setting-field">
                <div id="searchable-fields-container">
                    <div id="no-searchable-fields" class="unset"><fmt:message key="partials.provenance.provenance-search-dialog.no-searchable-fields"/>.</div>
                </div>
            </div>
        </div>
        <div class="setting">
            <div class="start-date-setting">
                <div class="setting-name">
                    <fmt:message key="partials.provenance.provenance-search-dialog.StartDate"/>
                    <fmt:message key="partials.provenance.provenance-search-dialog.StartDate.title" var="provenance-search-dialog_StartDate"/>
                    <div class="fa fa-question-circle" title="${provenance-search-dialog_StartDate}"></div>
                </div>
                <div class="setting-field">
                    <input type="text" id="provenance-search-start-date" class="provenance-small-input"/>
                </div>
            </div>
            <div class="start-time-setting">
                <div class="setting-name">
                    <fmt:message key="partials.provenance.provenance-search-dialog.StartTime"/>(<span class="timezone"></span>)
                    <fmt:message key="partials.provenance.provenance-search-dialog.StartTime.title" var="provenance-search-dialog_StartTime"/>
                    <div class="fa fa-question-circle" title="${provenance-search-dialog_StartTime}"></div>
                </div>
                <div class="setting-field">
                    <input type="text" id="provenance-search-start-time" class="provenance-time-input"/>
                </div>
            </div>
            <div class="clear"></div>
        </div>
        <div class="setting">
            <div class="end-date-setting">
                <div class="setting-name">
                    <fmt:message key="partials.provenance.provenance-search-dialog.EndDate"/>
                    <fmt:message key="partials.provenance.provenance-search-dialog.EndDate.title" var="provenance-search-dialog_EndDate"/>
                    <div class="fa fa-question-circle" title="${provenance-search-dialog_EndDate}"></div>
                </div>
                <div class="setting-field">
                    <input type="text" id="provenance-search-end-date" class="provenance-small-input"/>
                </div>
            </div>
            <div class="end-time-setting">
                <div class="setting-name">
                    <fmt:message key="partials.provenance.provenance-search-dialog.EndTime"/>(<span class="timezone"></span>)
                    <fmt:message key="partials.provenance.provenance-search-dialog.EndTime.title" var="provenance-search-dialog_EndTime"/>
                    <div class="fa fa-question-circle" title="${provenance-search-dialog_EndTime}"></div>
                </div>
                <div class="setting-field">
                    <input type="text" id="provenance-search-end-time" class="provenance-time-input"/>
                </div>
            </div>
            <div class="clear"></div>
        </div>
        <div class="setting">
            <div class="end-date-setting">
                <div class="setting-name">
                    <fmt:message key="partials.provenance.provenance-search-dialog.MinimumFileSize"/>
                    <fmt:message key="partials.provenance.provenance-search-dialog.MinimumFileSize.title" var="provenance-search-dialog_MinimumFileSize"/>
                    <div class="fa fa-question-circle" title="${provenance-search-dialog_MinimumFileSize}"></div>
                </div>
                <div class="setting-field">
                    <input type="text" id="provenance-search-minimum-file-size" class="provenance-small-input"/>
                </div>
            </div>
            <div class="end-time-setting">
                <div class="setting-name">
                    <fmt:message key="partials.provenance.provenance-search-dialog.MaximumFileSize"/>
                    <fmt:message key="partials.provenance.provenance-search-dialog.MaximumFileSize.title" var="provenance-search-dialog_MaximumFileSize"/>
                    <div class="fa fa-question-circle" title="${provenance-search-dialog_MaximumFileSize}"></div>
                </div>
                <div class="setting-field">
                    <input type="text" id="provenance-search-maximum-file-size" class="provenance-time-input"/>
                </div>
            </div>
            <div class="clear"></div>
        </div>
        <div id="provenance-search-location-container" class="setting">
            <div class="setting-name">
                <fmt:message key="partials.provenance.provenance-search-dialog.SearchLocation"/>
            </div>
            <div class="setting-field">
                <div id="provenance-search-location"></div>
            </div>
            <div class="clear"></div>
        </div>
    </div>
</div>
