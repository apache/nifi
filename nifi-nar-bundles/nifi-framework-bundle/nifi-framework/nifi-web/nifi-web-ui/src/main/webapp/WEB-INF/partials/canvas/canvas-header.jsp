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
                    id="processor-component"
                    class="component-button icon icon-processor"
                    ng-disabled="!appCtrl.nf.CanvasUtils.canWriteCurrentGroup();"
                    nf-draggable="appCtrl.serviceProvider.headerCtrl.toolboxCtrl.draggableComponentConfig(appCtrl.serviceProvider.headerCtrl.toolboxCtrl.processorComponent);">
                <span class="component-button-grip"></span>
            </button>
            <button title="{{appCtrl.serviceProvider.headerCtrl.toolboxCtrl.config.type.inputPort}}"
                    id="port-in-component"
                    class="component-button icon icon-port-in"
                    ng-disabled="!appCtrl.nf.CanvasUtils.canWriteCurrentGroup();"
                    nf-draggable="appCtrl.serviceProvider.headerCtrl.toolboxCtrl.draggableComponentConfig(appCtrl.serviceProvider.headerCtrl.toolboxCtrl.inputPortComponent);">
                <span class="component-button-grip"></span>
            </button>
            <button title="{{appCtrl.serviceProvider.headerCtrl.toolboxCtrl.config.type.outputPort}}"
                    id="port-out-component"
                    class="component-button icon icon-port-out"
                    ng-disabled="!appCtrl.nf.CanvasUtils.canWriteCurrentGroup();"
                    nf-draggable="appCtrl.serviceProvider.headerCtrl.toolboxCtrl.draggableComponentConfig(appCtrl.serviceProvider.headerCtrl.toolboxCtrl.outputPortComponent);">
                <span class="component-button-grip"></span>
            </button>
            <button title="{{appCtrl.serviceProvider.headerCtrl.toolboxCtrl.config.type.processGroup}}"
                    id="group-component"
                    class="component-button icon icon-group"
                    ng-disabled="!appCtrl.nf.CanvasUtils.canWriteCurrentGroup();"
                    nf-draggable="appCtrl.serviceProvider.headerCtrl.toolboxCtrl.draggableComponentConfig(appCtrl.serviceProvider.headerCtrl.toolboxCtrl.groupComponent);">
                <span class="component-button-grip"></span>
            </button>
            <button title="{{appCtrl.serviceProvider.headerCtrl.toolboxCtrl.config.type.remoteProcessGroup}}"
                    id="group-remote-component"
                    class="component-button icon icon-group-remote"
                    ng-disabled="!appCtrl.nf.CanvasUtils.canWriteCurrentGroup();"
                    nf-draggable="appCtrl.serviceProvider.headerCtrl.toolboxCtrl.draggableComponentConfig(appCtrl.serviceProvider.headerCtrl.toolboxCtrl.remoteGroupComponent);">
                <span class="component-button-grip"></span>
            </button>
            <button title="{{appCtrl.serviceProvider.headerCtrl.toolboxCtrl.config.type.funnel}}"
                    id="funnel-component"
                    class="component-button icon icon-funnel"
                    ng-disabled="!appCtrl.nf.CanvasUtils.canWriteCurrentGroup();"
                    nf-draggable="appCtrl.serviceProvider.headerCtrl.toolboxCtrl.draggableComponentConfig(appCtrl.serviceProvider.headerCtrl.toolboxCtrl.funnelComponent);">
                <span class="component-button-grip"></span>
            </button>
            <button title="{{appCtrl.serviceProvider.headerCtrl.toolboxCtrl.config.type.template}}"
                    id="template-component"
                    class="component-button icon icon-template"
                    ng-disabled="!appCtrl.nf.CanvasUtils.canWriteCurrentGroup();"
                    nf-draggable="appCtrl.serviceProvider.headerCtrl.toolboxCtrl.draggableComponentConfig(appCtrl.serviceProvider.headerCtrl.toolboxCtrl.templateComponent);">
                <span class="component-button-grip"></span>
            </button>
            <button title="{{appCtrl.serviceProvider.headerCtrl.toolboxCtrl.config.type.label}}"
                    id="label-component"
                    class="component-button icon icon-label"
                    ng-disabled="!appCtrl.nf.CanvasUtils.canWriteCurrentGroup();"
                    nf-draggable="appCtrl.serviceProvider.headerCtrl.toolboxCtrl.draggableComponentConfig(appCtrl.serviceProvider.headerCtrl.toolboxCtrl.labelComponent);">
                <span class="component-button-grip"></span>
            </button>
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
                          ng-click="appCtrl.serviceProvider.headerCtrl.loginCtrl.shell.launch();">log in</span>
                </div>
                <div id="logout-link-container" style="display: none;">
                    <span id="logout-link" class="link"
                          ng-click="appCtrl.serviceProvider.headerCtrl.logoutCtrl.logout();">log out</span>
                </div>
            </div>
            <md-menu md-position-mode="target-right target" md-offset="-1 44">
                <button md-menu-origin id="global-menu-button" ng-click="$mdMenu.open()">
                    <div class="fa fa-navicon"></div>
                </button>
                <md-menu-content id="global-menu-content">
                    <md-menu-item layout-align="space-around center">
                        <a id="reporting-link"
                           ng-click="appCtrl.serviceProvider.headerCtrl.globalMenuCtrl.summary.shell.launch();">
                            <i class="fa fa-table"></i>Summary
                        </a>
                    </md-menu-item>
                    <md-menu-item layout-align="space-around center">
                        <a id="counters-link"
                           ng-click="appCtrl.serviceProvider.headerCtrl.globalMenuCtrl.counters.shell.launch();"
                           ng-class="{disabled: !appCtrl.nf.Common.canAccessCounters()}">
                            <i class="icon icon-counter"></i>Counters
                        </a>
                    </md-menu-item>
                    <md-menu-item layout-align="space-around center">
                        <a id="bulletin-board-link"
                           ng-click="appCtrl.serviceProvider.headerCtrl.globalMenuCtrl.bulletinBoard.shell.launch();">
                            <i class="fa fa-sticky-note-o"></i>Bulletin Board
                        </a>
                    </md-menu-item>
                    <md-menu-divider></md-menu-divider>
                    <md-menu-item
                            layout-align="space-around center">
                        <a id="provenance-link"
                           ng-click="appCtrl.serviceProvider.headerCtrl.globalMenuCtrl.dataProvenance.shell.launch();"
                           ng-class="{disabled: !appCtrl.nf.Common.canAccessProvenance()}">
                            <i class="icon icon-provenance"></i>Data Provenance
                        </a>
                    </md-menu-item>
                    <md-menu-divider></md-menu-divider>
                    <md-menu-item layout-align="space-around center">
                        <a id="flow-settings-link"
                           ng-click="appCtrl.serviceProvider.headerCtrl.globalMenuCtrl.controllerSettings.shell.launch();">
                            <i class="fa fa-wrench"></i>Controller Settings
                        </a>
                    </md-menu-item>
                    <md-menu-item ng-if="appCtrl.serviceProvider.headerCtrl.globalMenuCtrl.cluster.visible();"
                                  layout-align="space-around center">
                        <a id="cluster-link"
                           ng-click="appCtrl.serviceProvider.headerCtrl.globalMenuCtrl.cluster.shell.launch();"
                           ng-class="{disabled: !appCtrl.nf.Common.canAccessController()}">
                            <i class="fa fa-cubes"></i>Cluster
                        </a>
                    </md-menu-item>
                    <md-menu-item layout-align="space-around center">
                        <a id="history-link"
                           ng-click="appCtrl.serviceProvider.headerCtrl.globalMenuCtrl.flowConfigHistory.shell.launch();">
                            <i class="fa fa-history"></i>Flow Configuration History
                        </a>
                    </md-menu-item>
                    <md-menu-divider ng-if="appCtrl.nf.CanvasUtils.isManagedAuthorizer()"></md-menu-divider>
                    <md-menu-item layout-align="space-around center" ng-if="appCtrl.nf.CanvasUtils.isManagedAuthorizer()">
                        <a id="users-link" layout="row"
                           ng-click="appCtrl.serviceProvider.headerCtrl.globalMenuCtrl.users.shell.launch();"
                           ng-class="{disabled: !(appCtrl.nf.Common.canAccessTenants())}">
                            <i class="fa fa-users"></i>Users
                        </a>
                    </md-menu-item>
                    <md-menu-item layout-align="space-around center" ng-if="appCtrl.nf.CanvasUtils.isManagedAuthorizer()">
                        <a id="policies-link" layout="row"
                           ng-click="appCtrl.serviceProvider.headerCtrl.globalMenuCtrl.policies.shell.launch();"
                           ng-class="{disabled: !(appCtrl.nf.Common.canAccessTenants() && appCtrl.nf.Common.canModifyPolicies())}">
                            <i class="fa fa-key"></i>Policies
                        </a>
                    </md-menu-item>
                    <md-menu-divider></md-menu-divider>
                    <md-menu-item layout-align="space-around center">
                        <a id="templates-link"
                           ng-click="appCtrl.serviceProvider.headerCtrl.globalMenuCtrl.templates.shell.launch();">
                            <i class="icon icon-template"></i>Templates
                        </a>
                    </md-menu-item>
                    <md-menu-divider></md-menu-divider>
                    <md-menu-item layout-align="space-around center">
                        <a id="help-link"
                           ng-click="appCtrl.serviceProvider.headerCtrl.globalMenuCtrl.help.shell.launch();">
                            <i class="fa fa-question-circle"></i>Help
                        </a>
                    </md-menu-item>
                    <md-menu-item layout-align="space-around center">
                        <a id="about-link"
                           ng-click="appCtrl.serviceProvider.headerCtrl.globalMenuCtrl.about.modal.show();">
                            <i class="fa fa-info-circle"></i>About
                        </a>
                    </md-menu-item>
                </md-menu-content>
            </md-menu>
        </div>
    </div>
</md-toolbar>