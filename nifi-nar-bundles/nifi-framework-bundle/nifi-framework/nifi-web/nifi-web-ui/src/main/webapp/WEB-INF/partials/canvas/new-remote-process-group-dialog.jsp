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
<div id="new-remote-process-group-dialog" class="hidden large-dialog">
    <div class="dialog-content">
        <div class="setting">
            <div class="setting-name"><fmt:message key="partials.canvas.new-remote-process-group-dialog.URL"/>
            <fmt:message key="partials.canvas.new-remote-process-group-dialog.circle" var="circle"/>
                <div class="fa fa-question-circle" alt="Info" title="${circle}"></div>
             </div>
            <div class="setting-field">
                <input id="new-remote-process-group-uris" type="text" placeholder="https://remotehost:8080/nifi"/>
            </div>
        </div>
        <div class="setting">
            <div class="remote-process-group-setting-left">
                <div class="setting-name">
                    <fmt:message key="partials.canvas.new-remote-process-group-dialog.Transport"/>
                <fmt:message key="partials.canvas.disable-controller-service-dialog.Scope.setting-field.title" var="Transport"/>
                <div class="fa fa-question-circle" alt="Info" title="${Transport}"></div>
                </div>
                <div class="setting-field">
                    <div id="new-remote-process-group-transport-protocol-combo"></div>
                </div>
            </div>
            <div class="remote-process-group-setting-right">
                <div class="setting-name">
                    <fmt:message key="partials.canvas.new-remote-process-group-dialog.LocalNetwork"/>
                <fmt:message key="partials.canvas.new-remote-process-group-dialog.LocalNetworkTitle" var="LocalNetworkTitle"/>
                    <div class="fa fa-question-circle" alt="Info" title="${LocalNetworkTitle}"></div>
                </div>
                <div class="setting-field">
                    <input type="text" class="small-setting-input" id="new-remote-process-group-local-network-interface"/>
                </div>
            </div>
            <div class="clear"></div>
        </div>
        <div class="setting">
            <div class="remote-process-group-setting-left">
                <div class="setting-name">
                    <fmt:message key="partials.canvas.new-remote-process-group-dialog.HttpProxyHostname"/>
                    <fmt:message key="partials.canvas.disable-controller-service-dialog.Scope.setting-field.title" var="HttpProxyHostname"/>
                    <div class="fa fa-question-circle" alt="Info" title="${HttpProxyHostname}"></div>
                </div>
                <div class="setting-field">
                    <input type="text" class="small-setting-input" id="new-remote-process-group-proxy-host"/>
                </div>
            </div>
            <div class="remote-process-group-setting-right">
                <div class="setting-name">
                    <fmt:message key="partials.canvas.new-remote-process-group-dialog.HttpProxyPort"/>
                    <fmt:message key="partials.canvas.disable-controller-service-dialog.Scope.setting-field.title" var="HttpProxyPort"/>
                    <div class="fa fa-question-circle" alt="Info" title="${HttpProxyPort}"></div>
                </div>
                <div class="setting-field">
                    <input type="text" class="small-setting-input" id="new-remote-process-group-proxy-port"/>
                </div>
            </div>
            <div class="clear"></div>
        </div>
        <div class="setting">
            <div class="remote-process-group-setting-left">
                <div class="setting-name">
                    <fmt:message key="partials.canvas.new-remote-process-group-dialog.HttpProxyUser"/>
                    <fmt:message key="partials.canvas.disable-controller-service-dialog.Scope.setting-field.title" var="HttpProxyUser"/>
                    <div class="fa fa-question-circle" alt="Info" title="${HttpProxyUser}"></div>
                </div>
                <div class="setting-field">
                    <input type="text" class="small-setting-input" id="new-remote-process-group-proxy-user"/>
                </div>
            </div>
            <div class="remote-process-group-setting-right">
                <div class="setting-name">
                    <fmt:message key="partials.canvas.new-remote-process-group-dialog.HttpProxyPassword"/>
                    <fmt:message key="partials.canvas.disable-controller-service-dialog.Scope.setting-field.title" var="HttpProxyPassword"/>
                    <div class="fa fa-question-circle" alt="Info" title="${HttpProxyPassword}"></div>
                </div>
                <div class="setting-field">
                    <input type="password" class="small-setting-input" id="new-remote-process-group-proxy-password"/>
                </div>
            </div>
            <div class="clear"></div>
        </div>
        <div class="setting">
            <div class="remote-process-group-setting-left">
                <div class="setting-name">
                    <fmt:message key="partials.canvas.new-remote-process-group-dialog.timeout"/>
                    <fmt:message key="partials.canvas.disable-controller-service-dialog.Scope.setting-field.title" var="timeout"/>
                    <div class="fa fa-question-circle" alt="Info" title="${timeout}"></div>
                </div>
                <div class="setting-field">
                    <input type="text" class="small-setting-input" id="new-remote-process-group-timeout"/>
                </div>
            </div>
            <div class="remote-process-group-setting-right">
                <div class="setting-name">
                    <fmt:message key="partials.canvas.new-remote-process-group-dialog.yield"/>
                    <fmt:message key="partials.canvas.disable-controller-service-dialog.Scope.setting-field.title" var="yield"/>
                    <div class="fa fa-question-circle" alt="Info" title="${yield}"></div>
                </div>
                <div class="setting-field">
                    <input type="text" class="small-setting-input" id="new-remote-process-group-yield-duration"/>
                </div>
            </div>
            <div class="clear"></div>
        </div>
    </div>
</div>