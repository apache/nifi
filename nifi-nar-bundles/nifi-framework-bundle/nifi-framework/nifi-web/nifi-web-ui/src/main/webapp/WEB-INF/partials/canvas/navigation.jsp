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
        click-func="appCtrl.nf.CanvasUtils.enterGroup"
        highlight-crumb-id="appCtrl.nf.Canvas.getGroupId();"
        separator-func="appCtrl.nf.Common.isDefinedAndNotNull">
</nf-breadcrumbs>
<div id="graph-controls">
    <div id="navigation-control" class="graph-control">
        <div class="graph-control-docked pointer" title="Navigate"
             ng-click="appCtrl.serviceProvider.graphControlsCtrl.undock($event)">
            <i class="graph-control-icon fa fa-compass"></i>
        </div>
        <div class="graph-control-header-container hidden">
            <div class="graph-control-header-icon">
                <i class="graph-control-icon fa fa-compass"></i>
            </div>
            <div class="graph-control-header">Navigate</div>
            <div class="graph-control-header-action"
                 ng-click="appCtrl.serviceProvider.graphControlsCtrl.expand($event)">
                <i class="graph-control-expansion fa fa-plus-square-o pointer"></i>
            </div>
            <div class="clear"></div>
        </div>
        <div class="graph-control-content hidden">
            <div id="navigation-buttons">
                <div id="naviagte-zoom-in" class="action-button" title="Zoom In"
                     ng-click="appCtrl.serviceProvider.graphControlsCtrl.navigateCtrl.zoomIn();">
                    <button><i class="graph-control-action-icon fa fa-search-plus"></i></button>
                </div>
                <div class="button-spacer-small">&nbsp;</div>
                <div id="naviagte-zoom-out" class="action-button" title="Zoom Out"
                     ng-click="appCtrl.serviceProvider.graphControlsCtrl.navigateCtrl.zoomOut();">
                    <button><i class="graph-control-action-icon fa fa-search-minus"></i></button>
                </div>
                <div class="button-spacer-large">&nbsp;</div>
                <div id="naviagte-zoom-fit" class="action-button" title="Fit"
                     ng-click="appCtrl.serviceProvider.graphControlsCtrl.navigateCtrl.zoomFit();">
                    <button><i class="graph-control-action-icon icon icon-zoom-fit"></i></button>
                </div>
                <div class="button-spacer-small">&nbsp;</div>
                <div id="naviagte-zoom-actual-size" class="action-button" title="Actual"
                     ng-click="appCtrl.serviceProvider.graphControlsCtrl.navigateCtrl.zoomActualSize();">
                    <button><i class="graph-control-action-icon icon icon-zoom-actual"></i></button>
                </div>
                <div class="clear"></div>
            </div>
            <div id="birdseye"></div>
        </div>
    </div>
    <div id="operation-control" class="graph-control">
        <div class="graph-control-docked pointer" title="Operate"
             ng-click="appCtrl.serviceProvider.graphControlsCtrl.undock($event)">
            <i class="graph-control-icon fa fa-hand-o-up"></i>
        </div>
        <div class="graph-control-header-container hidden">
            <div class="graph-control-header-icon">
                <i class="graph-control-icon fa fa-hand-o-up"></i>
            </div>
            <div class="graph-control-header">Operate</div>
            <div class="graph-control-header-action"
                 ng-click="appCtrl.serviceProvider.graphControlsCtrl.expand($event)">
                <i class="graph-control-expansion fa fa-plus-square-o pointer"></i>
            </div>
            <div class="clear"></div>
        </div>
        <div class="graph-control-content hidden">
            <div id="operation-buttons">
                <div>
                    <div id="operate-enable" class="action-button" title="Enable">
                        <button ng-click="appCtrl.nf.Actions['enable'](appCtrl.nf.CanvasUtils.getSelection());"
                                ng-disabled="appCtrl.nf.CanvasUtils.getSelection().empty() || !appCtrl.nf.CanvasUtils.canEnable(appCtrl.nf.CanvasUtils.getSelection());">
                            <i class="graph-control-action-icon fa fa-flash"></i></button>
                    </div>
                    <div class="button-spacer-small">&nbsp;</div>
                    <div id="operate-disable" class="action-button" title="Disable">
                        <button ng-click="appCtrl.nf.Actions['disable'](appCtrl.nf.CanvasUtils.getSelection());"
                                ng-disabled="appCtrl.nf.CanvasUtils.getSelection().empty() || !appCtrl.nf.CanvasUtils.canDisable(appCtrl.nf.CanvasUtils.getSelection());">
                            <i class="graph-control-action-icon icon icon-enable-false"></i></button>
                    </div>
                    <div class="button-spacer-large">&nbsp;</div>
                    <div id="operate-start" class="action-button" title="Start">
                        <button ng-click="appCtrl.nf.Actions['start'](appCtrl.nf.CanvasUtils.getSelection());"
                                ng-disabled="!appCtrl.nf.Common.isDFM();">
                            <i class="graph-control-action-icon fa fa-play"></i></button>
                    </div>
                    <div class="button-spacer-small">&nbsp;</div>
                    <div id="operate-stop" class="action-button" title="Stop">
                        <button ng-click="appCtrl.nf.Actions['stop'](appCtrl.nf.CanvasUtils.getSelection());"
                                ng-disabled="!appCtrl.nf.Common.isDFM();">
                            <i class="graph-control-action-icon fa fa-stop"></i></button>
                    </div>
                    <div class="button-spacer-large">&nbsp;</div>
                    <div id="operate-template" class="action-button" title="Create Template">
                        <button ng-click="appCtrl.nf.Actions['template'](appCtrl.nf.CanvasUtils.getSelection());"
                                ng-disabled="!appCtrl.nf.Common.isDFM();">
                            <i class="graph-control-action-icon icon icon-template"></i></button>
                    </div>
                    <div class="button-spacer-large">&nbsp;</div>
                    <div id="operate-copy" class="action-button" title="Copy">
                        <button ng-click="appCtrl.nf.Actions['copy'](appCtrl.nf.CanvasUtils.getSelection());"
                                ng-disabled="appCtrl.nf.CanvasUtils.getSelection().empty() || !appCtrl.nf.CanvasUtils.isCopyable(appCtrl.nf.CanvasUtils.getSelection());">
                            <i class="graph-control-action-icon fa fa-copy"></i></button>
                    </div>
                    <div class="button-spacer-small">&nbsp;</div>
                    <div id="operate-paste" class="action-button" title="Paste">
                        <button ng-click="appCtrl.nf.Actions['paste'](appCtrl.nf.CanvasUtils.getSelection());"
                                ng-disabled="!appCtrl.nf.Clipboard.isCopied() || !appCtrl.nf.CanvasUtils.canModify(appCtrl.nf.CanvasUtils.getSelection());">
                            <i class="graph-control-action-icon fa fa-paste"></i></button>
                    </div>
                    <div class="clear"></div>
                </div>
                <div style="margin-top: 5px;">
                    <div id="operate-group" class="action-button" title="Group">
                        <button ng-click="appCtrl.nf.Actions['group'](appCtrl.nf.CanvasUtils.getSelection());"
                                ng-disabled="appCtrl.nf.CanvasUtils.getSelection().empty() || !appCtrl.nf.CanvasUtils.isDisconnected(appCtrl.nf.CanvasUtils.getSelection());">
                            <i class="graph-control-action-icon icon icon-group"></i></button>
                    </div>
                    <div class="button-spacer-large">&nbsp;</div>
                    <div id="operate-color" class="action-button" title="Fill Color">
                        <button ng-click="appCtrl.nf.Actions['fillColor'](appCtrl.nf.CanvasUtils.getSelection());"
                                ng-disabled="appCtrl.nf.CanvasUtils.getSelection().empty() || !appCtrl.nf.CanvasUtils.isColorable(appCtrl.nf.CanvasUtils.getSelection());">
                            <i class="graph-control-action-icon fa fa-paint-brush"></i></button>
                    </div>
                    <div class="button-spacer-large">&nbsp;</div>
                    <div id="operate-delete" class="action-button" title="Delete">
                        <button ng-click="appCtrl.nf.Actions['delete'](appCtrl.nf.CanvasUtils.getSelection());"
                                ng-disabled="appCtrl.nf.CanvasUtils.getSelection().empty();">
                            <i class="graph-control-action-icon fa fa-trash"></i><span>Delete</span></button>
                    </div>
                    <div class="clear"></div>
                </div>
            </div>
        </div>
    </div>
</div>