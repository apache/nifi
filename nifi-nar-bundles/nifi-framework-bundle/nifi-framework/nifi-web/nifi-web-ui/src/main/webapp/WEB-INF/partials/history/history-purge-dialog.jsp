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
<div id="history-purge-dialog" class="hidden medium-dialog">
    <div class="dialog-content">
        <div class="setting" style="margin-bottom: 0px;">
            <div class="end-date-setting">
                <div class="setting-name">
                    <fmt:message key="partials.history.history-purge-dialog.EndDate"/>
                    <fmt:message key="partials.history.history-purge-dialog.Title1" var="title1"/>
                    <div class="fa fa-question-circle" alt="Info" title="${title1}"></div>
                </div>
                <div class="setting-field">
                    <input type="text" id="history-purge-end-date" class="history-small-input"/>
                </div>
            </div>
            <div class="end-time-setting">
                <div class="setting-name">
                    <fmt:message key="partials.history.history-purge-dialog.EndTime"/> (<span class="timezone"></span>)
                    <fmt:message key="partials.history.history-purge-dialog.Title2" var="title2"/>
                    <div class="fa fa-question-circle" id="purge-end-time-info" alt="Info" title="${title2}"></div>
                </div>
                <div class="setting-field">
                    <input type="text" id="history-purge-end-time" class="history-small-input"/>
                </div>
            </div>
            <div class="clear"></div>
        </div>
    </div>
</div>
