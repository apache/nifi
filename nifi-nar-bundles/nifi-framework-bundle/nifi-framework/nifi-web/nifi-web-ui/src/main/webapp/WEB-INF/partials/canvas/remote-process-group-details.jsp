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
<div id="remote-process-group-details" class="hidden medium-dialog">
    <div class="dialog-content">
        <div class="setting">
            <div class="setting-name"><fmt:message key="partials.canvas.remote-process-group-details.Name"/></div>
            <div class="setting-field">
                <span id="read-only-remote-process-group-name"></span>
            </div>
        </div>
        <div class="setting">
            <div class="setting-name"><fmt:message key="partials.canvas.remote-process-group-details.Id"/></div>
            <div class="setting-field">
                <span id="read-only-remote-process-group-id"></span>
            </div>
        </div>
        <div class="setting">
            <div class="setting-name"><fmt:message key="partials.canvas.remote-process-group-details.URL"/></div>
            <div class="setting-field">
                <span id="read-only-remote-process-group-urls"></span>
            </div>
        </div>
        <div class="setting">
            <div class="remote-process-group-setting-left">
                <div class="setting-name">
                <fmt:message key="partials.canvas.remote-process-group-details.TransportProtocol"/>
                    <fmt:message key="partials.canvas.remote-process-group-details.TransportProtocolTitle" var="TransportProtocol2"/>
                <div class="fa fa-question-circle" alt="Info" title="TransportProtocol${TransportProtocol2}"></div>
            </div>
                <div class="setting-field">
                    <div id="read-only-remote-process-group-transport-protocol"></div>
                </div>
            </div>
            <div class="remote-process-group-setting-right">
                <div class="setting-name">
                    <fmt:message key="partials.canvas.remote-process-group-details.LocalNetwork"/>
                    <fmt:message key="partials.canvas.remote-process-group-details.LocalNetworkTitle" var="LocalNetworkTitle2"/>
                    <div class="fa fa-question-circle" alt="Info" title="${LocalNetworkTitle2}"></div>
                </div>
                <div class="setting-field">
                    <span id="read-only-remote-process-group-local-network-interface"></span>
                </div>
            </div>
            <div class="clear"></div>
        </div>
        <div class="setting">
            <div class="remote-process-group-setting-left">
                <div class="setting-name">
                    <fmt:message key="partials.canvas.remote-process-group-details.hostname"/>
                    <fmt:message key="partials.canvas.remote-process-group-details.hostnameTitle" var="hostname2"/>
                    <div class="fa fa-question-circle" alt="Info" title="${hostname2}"></div>
                </div>
                <div class="setting-field">
                    <span id="read-only-remote-process-group-proxy-host"></span>
                </div>
            </div>
            <div class="remote-process-group-setting-right">
                <div class="setting-name">
                    <fmt:message key="partials.canvas.remote-process-group-details.port"/>
                    <fmt:message key="partials.canvas.remote-process-group-details.portTitle" var="porttitle2"/>
                    <div class="fa fa-question-circle" alt="Info" title="${porttitle2}"></div>
                </div>
                <div class="setting-field">
                    <span id="read-only-remote-process-group-proxy-port"></span>
                </div>
            </div>
            <div class="clear"></div>
        </div>
        <div class="setting">
            <div class="remote-process-group-setting-left">
                <div class="setting-name">
                    <fmt:message key="partials.canvas.remote-process-group-details.user"/>
                    <fmt:message key="partials.canvas.remote-process-group-details.userTitle" var="usertitle2"/>
                    <div class="fa fa-question-circle" alt="Info" title="${usertitle2}"></div>
                </div>
                <div class="setting-field">
                    <span id="read-only-remote-process-group-proxy-user"></span>
                </div>
            </div>
            <div class="remote-process-group-setting-right">
                <div class="setting-name">
                    <fmt:message key="partials.canvas.remote-process-group-details.pwd"/>
                    <fmt:message key="partials.canvas.remote-process-group-details.pwdTitle" var="pwdtitle2"/>
                    <div class="fa fa-question-circle" alt="Info" title="${pwdtitle2}"></div>
                </div>
                <div class="setting-field">
                    <span id="read-only-remote-process-group-proxy-password"></span>
                </div>
            </div>
            <div class="clear"></div>
        </div>
        <div class="setting">
            <div class="remote-process-group-setting-left">
                <div class="setting-name">
                    <fmt:message key="partials.canvas.remote-process-group-details.CommunicationsTimeout"/>
                    <fmt:message key="partials.canvas.remote-process-group-details.CommunicationsTimeoutTitle" var="CommunicationsTimeout2"/>
                    <div class="fa fa-question-circle" alt="Info" title="${CommunicationsTimeout2}"></div>
                </div>
                <div class="setting-field">
                    <span id="read-only-remote-process-group-timeout"></span>
                </div>
            </div>
            <div class="remote-process-group-setting-right">
                <div class="setting-name">
                    <fmt:message key="partials.canvas.remote-process-group-details.YieldDuration"/>
                    <fmt:message key="partials.canvas.remote-process-group-details.YieldDuration.title" var="YieldDuration2"/>
                    <div class="fa fa-question-circle" alt="Info" title="${YieldDuration2}"></div>
                </div>
                <div class="setting-field">
                    <span id="read-only-remote-process-group-yield-duration"></span>
                </div>
            </div>
            <div class="clear"></div>
        </div>
    </div>
</div>