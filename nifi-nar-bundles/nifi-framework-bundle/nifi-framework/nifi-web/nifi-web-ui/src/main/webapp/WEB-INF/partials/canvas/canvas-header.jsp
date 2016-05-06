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
<md-toolbar id="header" layout-align="space-between center" layout="row" class="md-small md-accent md-hue-1">
    <img id="nifi-logo" src="images/nifi-logo.svg">
    <div flex layout="row" layout-align="space-between center">
        <div id="component-container">
            <button title="{{appCtrl.serviceProvider.headerCtrl.toolboxCtrl.config.type.processor}}"
                    id="processor-component" class="component-button"
                    nf-draggable="appCtrl.serviceProvider.headerCtrl.toolboxCtrl.draggableComponentConfig(appCtrl.serviceProvider.headerCtrl.toolboxCtrl.processorComponent);">
                <i class="icon icon-processor"></i></button>
            <button title="{{appCtrl.serviceProvider.headerCtrl.toolboxCtrl.config.type.inputPort}}"
                    id="port-in-component" class="component-button"
                    nf-draggable="appCtrl.serviceProvider.headerCtrl.toolboxCtrl.draggableComponentConfig(appCtrl.serviceProvider.headerCtrl.toolboxCtrl.inputPortComponent);">
                <i class="icon icon-port-in"></i></button>
            <button title="{{appCtrl.serviceProvider.headerCtrl.toolboxCtrl.config.type.outputPort}}"
                    id="port-out-component" class="component-button"
                    nf-draggable="appCtrl.serviceProvider.headerCtrl.toolboxCtrl.draggableComponentConfig(appCtrl.serviceProvider.headerCtrl.toolboxCtrl.outputPortComponent);">
                <i class="icon icon-port-out"></i></button>
            <button title="{{appCtrl.serviceProvider.headerCtrl.toolboxCtrl.config.type.processGroup}}"
                    id="group-component" class="component-button"
                    nf-draggable="appCtrl.serviceProvider.headerCtrl.toolboxCtrl.draggableComponentConfig(appCtrl.serviceProvider.headerCtrl.toolboxCtrl.groupComponent);">
                <i class="icon icon-group"></i></button>
            <button title="{{appCtrl.serviceProvider.headerCtrl.toolboxCtrl.config.type.remoteProcessGroup}}" hide
                    show-gt-sm id="group-remote-component" class="component-button"
                    nf-draggable="appCtrl.serviceProvider.headerCtrl.toolboxCtrl.draggableComponentConfig(appCtrl.serviceProvider.headerCtrl.toolboxCtrl.remoteGroupComponent);">
                <i class="icon icon-group-remote"></i></button>
            <button title="{{appCtrl.serviceProvider.headerCtrl.toolboxCtrl.config.type.funnel}}"
                    id="funnel-component" class="component-button"
                    nf-draggable="appCtrl.serviceProvider.headerCtrl.toolboxCtrl.draggableComponentConfig(appCtrl.serviceProvider.headerCtrl.toolboxCtrl.funnelComponent);">
                <i class="icon icon-funnel"></i></button>
            <button title="{{appCtrl.serviceProvider.headerCtrl.toolboxCtrl.config.type.template}}"
                    id="template-component" class="component-button"
                    nf-draggable="appCtrl.serviceProvider.headerCtrl.toolboxCtrl.draggableComponentConfig(appCtrl.serviceProvider.headerCtrl.toolboxCtrl.templateComponent);">
                <i class="icon icon-template"></i></button>
            <button title="{{appCtrl.serviceProvider.headerCtrl.toolboxCtrl.config.type.label}}"
                    id="label-component" class="component-button"
                    nf-draggable="appCtrl.serviceProvider.headerCtrl.toolboxCtrl.draggableComponentConfig(appCtrl.serviceProvider.headerCtrl.toolboxCtrl.labelComponent);">
                <i class="icon icon-label"></i></button>
        </div>
        <div layout="row" layout-align="space-between center">
            <div layout-align="space-between end" layout="column">
                <div layout="row" layout-align="space-between center" id="current-user-container">
                    <span id="anonymous-user-alert" class="hidden fa fa-warning"></span>
                    <div></div>
                    <div id="current-user"></div>
                </div>
                <div id="login-link-container">
                    <span id="login-link" class="link"
                          ng-click="appCtrl.serviceProvider.headerCtrl.loginLink.shell.launch();">login</span>
                </div>
                <div id="logout-link-container" style="display: none;">
                    <span id="logout-link" class="link"
                          ng-click="appCtrl.serviceProvider.headerCtrl.logoutLink.logout();">logout</span>
                </div>
            </div>
            <md-menu md-position-mode="target-right target" md-offset="-1 44">
                <button md-menu-origin id="global-menu-button" ng-click="$mdOpenMenu()"><i class="fa fa-navicon"></i>
                </button>
                <md-menu-content id="global-menu-content">
                    <md-menu-item layout-align="space-around center">
                        <a id="reporting-link"
                           ng-click="appCtrl.serviceProvider.headerCtrl.globalMenuCtrl.summary.shell.launch();"><i
                                class="fa fa-table"></i>Summary</a>
                    </md-menu-item>
                    <md-menu-item layout-align="space-around center">
                        <a id="counters-link"
                           ng-click="appCtrl.serviceProvider.headerCtrl.globalMenuCtrl.counters.shell.launch();"><i
                                class="icon icon-counter"></i>Counters</a>
                    </md-menu-item>
                    <md-menu-item layout-align="space-around center">
                        <a id="bulletin-board-link"
                           ng-click="appCtrl.serviceProvider.headerCtrl.globalMenuCtrl.bulletinBoard.shell.launch();"><i
                                class="fa fa-sticky-note-o"></i>Bulletin Board</a>
                    </md-menu-item>
                    <md-menu-divider></md-menu-divider>
                    <md-menu-item
                            ng-if="appCtrl.serviceProvider.headerCtrl.globalMenuCtrl.dataProvenance.enabled();"
                            layout-align="space-around center">
                        <a id="provenance-link"
                           ng-click="appCtrl.serviceProvider.headerCtrl.globalMenuCtrl.dataProvenance.shell.launch();"><i
                                class="icon icon-provenance"></i>Data Provenance</a>
                    </md-menu-item>
                    <md-menu-divider></md-menu-divider>
                    <md-menu-item layout-align="space-around center">
                        <a id="flow-settings-link"
                           ng-click="appCtrl.serviceProvider.headerCtrl.globalMenuCtrl.controllerSettings.shell.launch();"><i
                                class="fa fa-wrench"></i>Controller Settings</a>
                    </md-menu-item>
                    <md-menu-item ng-if="appCtrl.serviceProvider.headerCtrl.globalMenuCtrl.cluster.enabled();"
                                  layout-align="space-around center">
                        <a id="cluster-link"
                           ng-click="appCtrl.serviceProvider.headerCtrl.globalMenuCtrl.cluster.shell.launch();"><i
                                class="fa fa-cubes"></i>Cluster</a>
                    </md-menu-item>
                    <md-menu-item layout-align="space-around center">
                        <a id="history-link"
                           ng-click="appCtrl.serviceProvider.headerCtrl.globalMenuCtrl.flowConfigHistory.shell.launch();"><i
                                class="fa fa-history"></i>Flow Configuration History</a>
                    </md-menu-item>
                    <md-menu-item ng-if="appCtrl.serviceProvider.headerCtrl.globalMenuCtrl.users.enabled();"
                                  layout-align="space-around center">
                        <a id="users-link"
                           ng-click="appCtrl.serviceProvider.headerCtrl.globalMenuCtrl.users.shell.launch();;"><i
                                class="fa fa-users"></i>Users</a>
                    </md-menu-item>
                    <md-menu-divider></md-menu-divider>
                    <md-menu-item layout-align="space-around center">
                        <a id="templates-link"
                           ng-click="appCtrl.serviceProvider.headerCtrl.globalMenuCtrl.templates.shell.launch();"><i
                                class="icon icon-template"></i>Templates</a>
                    </md-menu-item>
                    <md-menu-divider></md-menu-divider>
                    <md-menu-item layout-align="space-around center">
                        <a id="help-link"
                           ng-click="appCtrl.serviceProvider.headerCtrl.globalMenuCtrl.help.shell.launch();"><i
                                class="fa fa-question-circle"></i>Help</a>
                    </md-menu-item>
                    <md-menu-item layout-align="space-around center">
                        <a id="about-link"
                           ng-click="appCtrl.serviceProvider.headerCtrl.globalMenuCtrl.about.modal.show();"><i
                                class="fa fa-info-circle"></i>About</a>
                    </md-menu-item>
                </md-menu-content>
            </md-menu>
        </div>
    </div>
</md-toolbar>