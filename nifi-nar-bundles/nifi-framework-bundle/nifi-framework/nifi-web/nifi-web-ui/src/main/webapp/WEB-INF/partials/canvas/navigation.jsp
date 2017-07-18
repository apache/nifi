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
<nf-breadcrumbs
        breadcrumbs="appCtrl.serviceProvider.breadcrumbsCtrl.getBreadcrumbs();"
        click-func="appCtrl.nf.CanvasUtils.getComponentByType('ProcessGroup').enterGroup"
        highlight-crumb-id="appCtrl.nf.CanvasUtils.getGroupId();"
        separator-func="appCtrl.nf.Common.isDefinedAndNotNull">
</nf-breadcrumbs>
<div id="graph-controls">
    <div id="navigation-control" class="graph-control">
        <div class="graph-control-docked pointer fa fa-compass" title="Navigate"
             ng-click="appCtrl.serviceProvider.graphControlsCtrl.undock($event)">
        </div>
        <div class="graph-control-header-container hidden pointer"
             ng-click="appCtrl.serviceProvider.graphControlsCtrl.expand($event)">
            <div class="graph-control-header-icon fa fa-compass">
            </div>
            <div class="graph-control-header">Navigate</div>
            <div class="graph-control-header-action">
                <div class="graph-control-expansion fa fa-plus-square-o pointer"></div>
            </div>
            <div class="clear"></div>
        </div>
        <div class="graph-control-content hidden">
            <div id="navigation-buttons">
                <div id="naviagte-zoom-in" class="action-button" title="Zoom In"
                     ng-click="appCtrl.serviceProvider.graphControlsCtrl.navigateCtrl.zoomIn();">
                    <button><div class="graph-control-action-icon fa fa-search-plus"></div></button>
                </div>
                <div class="button-spacer-small">&nbsp;</div>
                <div id="naviagte-zoom-out" class="action-button" title="Zoom Out"
                     ng-click="appCtrl.serviceProvider.graphControlsCtrl.navigateCtrl.zoomOut();">
                    <button><div class="graph-control-action-icon fa fa-search-minus"></div></button>
                </div>
                <div class="button-spacer-large">&nbsp;</div>
                <div id="naviagte-zoom-fit" class="action-button" title="Fit"
                     ng-click="appCtrl.serviceProvider.graphControlsCtrl.navigateCtrl.zoomFit();">
                    <button><div class="graph-control-action-icon icon icon-zoom-fit"></div></button>
                </div>
                <div class="button-spacer-small">&nbsp;</div>
                <div id="naviagte-zoom-actual-size" class="action-button" title="Actual"
                     ng-click="appCtrl.serviceProvider.graphControlsCtrl.navigateCtrl.zoomActualSize();">
                    <button><div class="graph-control-action-icon icon icon-zoom-actual"></div></button>
                </div>
                <div class="clear"></div>
            </div>
            <div id="birdseye"></div>
        </div>
    </div>
    <div id="operation-control" class="graph-control">
        <div class="graph-control-docked pointer fa fa-hand-o-up" title="Operate"
             ng-click="appCtrl.serviceProvider.graphControlsCtrl.undock($event)">
        </div>
        <div class="graph-control-header-container hidden pointer"
             ng-click="appCtrl.serviceProvider.graphControlsCtrl.expand($event)">
            <div class="graph-control-header-icon fa fa-hand-o-up">
            </div>
            <div class="graph-control-header">Operate</div>
            <div class="graph-control-header-action">
                <div class="graph-control-expansion fa fa-plus-square-o pointer"></div>
            </div>
            <div class="clear"></div>
        </div>
        <div class="graph-control-content hidden">
            <div id="operation-context">
                <div id="operation-context-logo">
                    <i class="icon" ng-class="appCtrl.serviceProvider.graphControlsCtrl.getContextIcon()"></i>
                </div>
                <div id="operation-context-details-container">
                    <div id="operation-context-name">{{appCtrl.serviceProvider.graphControlsCtrl.getContextName()}}</div>
                    <div id="operation-context-type" ng-class="appCtrl.serviceProvider.graphControlsCtrl.hide()">{{appCtrl.serviceProvider.graphControlsCtrl.getContextType()}}</div>
                </div>
                <div class="clear"></div>
                <div id="operation-context-id" ng-class="appCtrl.serviceProvider.graphControlsCtrl.hide()">{{appCtrl.serviceProvider.graphControlsCtrl.getContextId()}}</div>
            </div>
            <div id="operation-buttons">
                <div>
                    <div id="operate-configure" class="action-button" title="Configuration">
                        <button ng-click="appCtrl.serviceProvider.graphControlsCtrl.openConfigureOrDetailsView();"
                                ng-disabled="!(appCtrl.serviceProvider.graphControlsCtrl.canConfigureOrOpenDetails())">
                            <div class="graph-control-action-icon fa fa-gear"></div></button>
                    </div>
                    <div class="button-spacer-small" ng-if="appCtrl.nf.CanvasUtils.isManagedAuthorizer()">&nbsp;</div>
                    <div id="operate-policy" class="action-button" title="Access Policies" ng-if="appCtrl.nf.CanvasUtils.isManagedAuthorizer()">
                        <button ng-click="appCtrl.nf.Actions['managePolicies'](appCtrl.nf.CanvasUtils.getSelection());"
                                ng-disabled="!(appCtrl.nf.CanvasUtils.canManagePolicies())">
                            <div class="graph-control-action-icon fa fa-key"></div></button>
                    </div>
                    <div class="button-spacer-large">&nbsp;</div>
                    <div id="operate-enable" class="action-button" title="Enable">
                        <button ng-click="appCtrl.nf.Actions['enable'](appCtrl.nf.CanvasUtils.getSelection());"
                                ng-disabled="!appCtrl.nf.CanvasUtils.canEnable(appCtrl.nf.CanvasUtils.getSelection());">
                            <div class="graph-control-action-icon fa fa-flash"></div></button>
                    </div>
                    <div class="button-spacer-small">&nbsp;</div>
                    <div id="operate-disable" class="action-button" title="Disable">
                        <button ng-click="appCtrl.nf.Actions['disable'](appCtrl.nf.CanvasUtils.getSelection());"
                                ng-disabled="!appCtrl.nf.CanvasUtils.canDisable(appCtrl.nf.CanvasUtils.getSelection());">
                            <div class="graph-control-action-icon icon icon-enable-false"></div></button>
                    </div>
                    <div class="button-spacer-large">&nbsp;</div>
                    <div id="operate-start" class="action-button" title="Start">
                        <button ng-click="appCtrl.nf.Actions['start'](appCtrl.nf.CanvasUtils.getSelection());"
                                ng-disabled="!appCtrl.nf.CanvasUtils.getSelection().empty() && !appCtrl.nf.CanvasUtils.canModify(appCtrl.nf.CanvasUtils.getSelection());">
                            <div class="graph-control-action-icon fa fa-play"></div></button>
                    </div>
                    <div class="button-spacer-small">&nbsp;</div>
                    <div id="operate-stop" class="action-button" title="Stop">
                        <button ng-click="appCtrl.nf.Actions['stop'](appCtrl.nf.CanvasUtils.getSelection());"
                                ng-disabled="!appCtrl.nf.CanvasUtils.getSelection().empty() && !appCtrl.nf.CanvasUtils.canModify(appCtrl.nf.CanvasUtils.getSelection());">
                            <div class="graph-control-action-icon fa fa-stop"></div></button>
                    </div>
                    <div class="button-spacer-large">&nbsp;</div>
                    <div id="operate-template" class="action-button" title="Create Template">
                        <button ng-click="appCtrl.nf.Actions['template'](appCtrl.nf.CanvasUtils.getSelection());"
                                ng-disabled="!(appCtrl.nf.CanvasUtils.canWrite() && (appCtrl.nf.CanvasUtils.getSelection().empty() || appCtrl.nf.CanvasUtils.canRead(appCtrl.nf.CanvasUtils.getSelection())));">
                            <div class="graph-control-action-icon icon icon-template-save"></div></button>
                    </div>
                    <div class="button-spacer-small">&nbsp;</div>
                    <div id="operate-template-upload" class="action-button" title="Upload Template">
                        <button ng-click="appCtrl.nf.Actions['uploadTemplate']();"
                                ng-disabled="!(appCtrl.nf.CanvasUtils.canWrite() && appCtrl.nf.CanvasUtils.getSelection().empty());">
                            <div class="graph-control-action-icon icon icon-template-import"></div></button>
                    </div>
                    <div class="clear"></div>
                </div>
                <div style="margin-top: 5px;">
                    <div id="operate-copy" class="action-button" title="Copy">
                        <button ng-click="appCtrl.nf.Actions['copy'](appCtrl.nf.CanvasUtils.getSelection());"
                                ng-disabled="!appCtrl.nf.CanvasUtils.isCopyable(appCtrl.nf.CanvasUtils.getSelection());">
                            <div class="graph-control-action-icon fa fa-copy"></div></button>
                    </div>
                    <div class="button-spacer-small">&nbsp;</div>
                    <div id="operate-paste" class="action-button" title="Paste">
                        <button ng-click="appCtrl.nf.Actions['paste'](appCtrl.nf.CanvasUtils.getSelection());"
                                ng-disabled="!appCtrl.nf.CanvasUtils.isPastable()">
                            <div class="graph-control-action-icon fa fa-paste"></div></button>
                    </div>
                    <div class="button-spacer-large">&nbsp;</div>
                    <div id="operate-group" class="action-button" title="Group">
                        <button ng-click="appCtrl.nf.Actions['group'](appCtrl.nf.CanvasUtils.getSelection());"
                                ng-disabled="!(appCtrl.nf.CanvasUtils.getComponentByType('Connection').isDisconnected(appCtrl.nf.CanvasUtils.getSelection()) && appCtrl.nf.CanvasUtils.canModify(appCtrl.nf.CanvasUtils.getSelection()));">
                            <div class="graph-control-action-icon icon icon-group"></div></button>
                    </div>
                    <div class="button-spacer-large">&nbsp;</div>
                    <div id="operate-color" class="action-button" title="Change Color">
                        <button ng-click="appCtrl.nf.Actions['fillColor'](appCtrl.nf.CanvasUtils.getSelection());"
                                ng-disabled="!appCtrl.nf.CanvasUtils.isColorable(appCtrl.nf.CanvasUtils.getSelection());">
                            <div class="graph-control-action-icon fa fa-paint-brush"></div></button>
                    </div>
                    <div class="button-spacer-large">&nbsp;</div>
                    <div id="operate-delete" class="action-button" title="Delete">
                        <button ng-click="appCtrl.nf.Actions['delete'](appCtrl.nf.CanvasUtils.getSelection());"
                                ng-disabled="!appCtrl.nf.CanvasUtils.areDeletable(appCtrl.nf.CanvasUtils.getSelection());">
                            <div class="graph-control-action-icon fa fa-trash"></div><span>Delete</span></button>
                    </div>
                    <div class="clear"></div>
                </div>
            </div>
        </div>
    </div>
</div>