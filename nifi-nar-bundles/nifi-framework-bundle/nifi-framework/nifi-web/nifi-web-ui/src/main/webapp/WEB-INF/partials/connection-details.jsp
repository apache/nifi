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
<div id="connection-details">
    <div class="connection-details-tab-container dialog-content">
        <div id="connection-details-tabs" class="tab-container"></div>
        <div id="connection-details-tabs-content">
            <div id="read-only-connection-details-tab-content" class="configuration-tab">
                <div class="settings-left">
                    <div class="setting">
                        <div id="read-only-connection-source-label" class="setting-name"></div>
                        <div class="setting-field connection-terminal-label">
                            <div id="read-only-connection-source" class="ellipsis"></div>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name"><fmt:message key="partials.connection-details.WithinGroup"/></div>
                        <div class="setting-field">
                            <div id="read-only-connection-source-group-name"></div>
                        </div>
                    </div>
                    <div id="read-only-relationship-names-container" class="setting">
                        <div class="setting-name">
                            <fmt:message key="partials.connection-details.Relationships"/>
                            <fmt:message key="partials.connection-details.configuration-tab.read-only-relationship-names-container.title" var="read-only-relationship-names-container_title"/>
                            <div class="fa fa-question-circle" alt="Info" title="${read-only-relationship-names-container_title}"></div>
                        </div>
                        <div class="setting-field">
                            <div id="read-only-relationship-names"></div>
                        </div>
                    </div>
                </div>
                <div class="spacer">&nbsp;</div>
                <div class="settings-right">
                    <div class="setting">
                        <div id="read-only-connection-target-label" class="setting-name"></div>
                        <div class="setting-field connection-terminal-label">
                            <div id="read-only-connection-target" class="ellipsis"></div>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name"><fmt:message key="partials.connection-details.WithinGroup"/></div>
                        <div class="setting-field">
                            <div id="read-only-connection-target-group-name"></div>
                        </div>
                    </div>
                </div>
            </div>
            <div id="read-only-connection-settings-tab-content" class="configuration-tab">
                <div class="settings-left">
                    <div class="setting">
                        <div class="setting-name"><fmt:message key="partials.connection-details.Name"/></div>
                        <div class="setting-field">
                            <span id="read-only-connection-name"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name"><fmt:message key="partials.connection-details.Id"/></div>
                        <div class="setting-field">
                            <span id="read-only-connection-id"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">
                            <fmt:message key="partials.connection-details.read-only-flow-file-expiration"/>
                            <fmt:message key="partials.connection-details.read-only-flow-file-expiration.title" var="read-only-flow-file-expiration_title"/>
                            <div class="fa fa-question-circle" alt="Info" title="${read-only-flow-file-expiration_title}"></div>
                        </div>
                        <div class="setting-field">
                            <span id="read-only-flow-file-expiration"></span>
                        </div>
                        <div class="clear"></div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">
                            <fmt:message key="partials.connection-details.read-only-back-pressure-object-threshold"/>
                            <fmt:message key="partials.connection-details.read-only-back-pressure-object-threshold.title" var="read-only-back-pressure-object-threshold_title"/>
                            <div class="fa fa-question-circle" alt="Info" title="${read-only-back-pressure-object-threshold_title}"></div>
                        </div>
                        <div class="setting-field">
                            <span id="read-only-back-pressure-object-threshold"></span>
                        </div>
                        <div class="clear"></div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">
                            <fmt:message key="partials.connection-details.read-only-back-pressure-data-size-threshold"/>
                            <fmt:message key="partials.connection-details.read-only-back-pressure-data-size-threshold.title" var="read-only-back-pressure-data-size-threshold_title"/>
                            <div class="fa fa-question-circle" alt="Info" title="${read-only-back-pressure-data-size-threshold_title}"></div>
                        </div>
                        <div class="setting-field">
                            <span id="read-only-back-pressure-data-size-threshold"></span>
                        </div>
                        <div class="clear"></div>
                    </div>
                </div>
                <div class="spacer">&nbsp;</div>
                <div class="settings-right">
                    <div class="setting">
                        <div class="setting-name">
                            <fmt:message key="partials.connection-details.read-only-prioritizers"/>
                            <fmt:message key="partials.connection-details.read-only-prioritizers.title" var="read-only-prioritizers_title"/>
                            <div class="fa fa-question-circle" alt="Info" title="${read-only-prioritizers_title}"></div>
                        </div>
                        <div class="setting-field">
                            <div id="read-only-prioritizers"></div>
                        </div>
                        <div class="clear"></div>
                    </div>
                </div>
            </div>
        </div>
    </div>
</div>