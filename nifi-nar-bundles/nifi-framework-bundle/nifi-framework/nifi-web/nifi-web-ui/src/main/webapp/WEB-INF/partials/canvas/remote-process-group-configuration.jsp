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
<div id="remote-process-group-configuration" class="hidden large-dialog">
    <div class="dialog-content">
        <div class="setting">
            <div class="setting-name"><fmt:message key="partials.canvas.remote-process-group-configuration.Name"/></div>
            <div class="setting-field">
                <span id="remote-process-group-name"></span>
            </div>
        </div>
        <div class="setting">
            <div class="setting-name"><fmt:message key="partials.canvas.remote-process-group-configuration.Id"/></div>
            <div class="setting-field">
                <span id="remote-process-group-id"></span>
            </div>
        </div>
        <div class="setting">
            <div class="setting-name"><fmt:message key="partials.canvas.remote-process-group-configuration.URL"/></div>
            <div class="setting-field">
                <span id="remote-process-group-urls"></span>
            </div>
        </div>
        <div class="setting">
            <div class="remote-process-group-setting-left">
                <div class="setting-name">
                    <fmt:message key="partials.canvas.remote-process-group-configuration.TransportProtocol"/>
                <fmt:message key="partials.canvas.remote-process-group-configuration.TransportProtocolTitle" var="TransportProtocol"/>
                    <div class="fa fa-question-circle" alt="Info" title="${TransportProtocol}"></div>
                </div>
                <div class="setting-field">
                    <div id="remote-process-group-transport-protocol-combo"></div>
                </div>
            </div>
            <div class="remote-process-group-setting-right">
                <div class="setting-name">
                    <fmt:message key="partials.canvas.remote-process-group-configuration.LocalNetwork"/>
                    <fmt:message key="partials.canvas.remote-process-group-configuration.LocalNetworkTitle" var="LocalNetworkTitle"/>
                    <div class="fa fa-question-circle" alt="Info" title="${LocalNetworkTitle}"></div>
                </div>
                <div class="setting-field">
                    <input type="text" class="small-setting-input" id="remote-process-group-local-network-interface"/>
                </div>
            </div>
            <div class="clear"></div>
        </div>
        <div class="setting">
            <div class="remote-process-group-setting-left">
                <div class="setting-name">
                    <fmt:message key="partials.canvas.remote-process-group-configuration.hostname"/>
                    <fmt:message key="partials.canvas.remote-process-group-configuration.hostnameTitle" var="hostnametitle"/>
                    <div class="fa fa-question-circle" alt="Info" title="${hostnametitle}"></div>
                </div>
                <div class="setting-field">
                    <input type="text" class="small-setting-input" id="remote-process-group-proxy-host"/>
                </div>
            </div>
            <div class="remote-process-group-setting-right">
                <div class="setting-name">
                    <fmt:message key="partials.canvas.remote-process-group-configuration.port"/>
                    <fmt:message key="partials.canvas.remote-process-group-configuration.portTitle" var="porttitle"/>
                    <div class="fa fa-question-circle" alt="Info" title="${porttitle}"></div>
                </div>
                <div class="setting-field">
                    <input type="text" class="small-setting-input" id="remote-process-group-proxy-port"/>
                </div>
            </div>
            <div class="clear"></div>
        </div>
        <div class="setting">
            <div class="remote-process-group-setting-left">
                <div class="setting-name">
                    <fmt:message key="partials.canvas.remote-process-group-configuration.user"/>
                    <fmt:message key="partials.canvas.remote-process-group-configuration.userTitle" var="usertitle"/>
                    <div class="fa fa-question-circle" alt="Info" title="${usertitle}"></div>
                </div>
                <div class="setting-field">
                    <input type="text" class="small-setting-input" id="remote-process-group-proxy-user"/>
                </div>
            </div>
            <div class="remote-process-group-setting-right">
                <div class="setting-name">
                    <fmt:message key="partials.canvas.remote-process-group-configuration.pwd"/>
                    <fmt:message key="partials.canvas.remote-process-group-configuration.pwdTitle" var="pwdtitle"/>
                    <div class="fa fa-question-circle" alt="Info" title="${pwdtitle}"></div>
                </div>
                <div class="setting-field">
                    <input type="password" class="small-setting-input" id="remote-process-group-proxy-password"/>
                </div>
            </div>
            <div class="clear"></div>
        </div>
        <div class="setting">
            <div class="remote-process-group-setting-left">
                <div class="setting-name">
                    <fmt:message key="partials.canvas.remote-process-group-configuration.CommunicationsTimeout"/>
                    <fmt:message key="partials.canvas.remote-process-group-configuration.CommunicationsTimeoutTitle" var="CommunicationsTimeout1"/>
                    <div class="fa fa-question-circle" alt="Info" title="${CommunicationsTimeout1}"></div>
                </div>
                <div class="setting-field">
                    <input type="text" class="small-setting-input" id="remote-process-group-timeout"/>
                </div>
            </div>
            <div class="remote-process-group-setting-right">
                <div class="setting-name">
                    <fmt:message key="partials.canvas.remote-process-group-configuration.YieldDuration"/>
                    <fmt:message key="partials.canvas.remote-process-group-configuration.YieldDuration.title" var="YieldDuration1"/>
                    <div class="fa fa-question-circle" alt="Info" title="${YieldDuration1}"></div>
                </div>
                <div class="setting-field">
                    <input type="text" class="small-setting-input" id="remote-process-group-yield-duration"/>
                </div>
            </div>
            <div class="clear"></div>
        </div>
    </div>
</div>