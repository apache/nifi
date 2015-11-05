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
<div id="header">
    <div id="nf-logo"></div>
    <div id="nf-logo-name"></div>
    <div id="toolbox-container">
        <div id="toolbox"></div>
        <div id="toolbox-right-edge"></div>
    </div>
    <div id="toolbar">
        <div id="global-controls"></div>
        <div id="utilities-container">
            <div id="utilities-border"></div>
            <div id="utility-buttons">
                <div id="reporting-link" class="utility-button" title="Summary"></div>
                <div id="counters-link" class="utility-button" title="Counters"></div>
                <div id="history-link" class="utility-button" title="Flow Configuration History"></div>
                <div id="provenance-link" class="utility-button" title="Data Provenance"></div>
                <div id="flow-settings-link" class="utility-button" title="Controller Settings"></div>
                <div id="templates-link" class="utility-button" title="Templates"></div>
                <div id="users-link" class="utility-button" title="Users"><div id="has-pending-accounts" class="hidden"></div></div>
                <div id="cluster-link" class="utility-button" title="Cluster"></div>
                <div id="bulletin-board-link" class="utility-button" title="Bulletin Board"></div>
            </div>
        </div>
        <div id="search-container">
            <input id="search-field" type="text"/>
        </div>
    </div>
    <div id="header-links-container">
        <ul>
            <li id="current-user-container">
                <div id="anonymous-user-alert"></div>
                <div id="current-user"></div>
                <div class="clear"></div>
            </li>
            <li id="login-link-container">
                <span id="login-link" class="link">login</span>
            </li>
            <li id="logout-link-container" style="display: none;">
                <span id="logout-link" class="link">logout</span>
            </li>
            <li>
                <span id="help-link" class="link">help</span>
            </li>
            <li>
                <span id="about-link" class="link">about</span>
            </li>
        </ul>
    </div>
</div>
<div id="search-flow-results"></div>
