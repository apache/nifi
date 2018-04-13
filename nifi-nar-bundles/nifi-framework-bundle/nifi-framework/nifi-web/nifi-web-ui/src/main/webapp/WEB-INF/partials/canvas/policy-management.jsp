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
<div id="policy-management">
    <div id="policy-management-header-text">Access Policies</div>
    <div id="policy-controls-container">
        <span id="selected-policy-action" class="hidden"></span>
        <span id="selected-policy-type" class="hidden"></span>
        <div id="policy-message-container">
            <div id="policy-message"></div>
            <div id="new-policy-message" class="hidden"><span id="create-policy-link" class="link">Create</span> a new policy.</div>
            <div id="override-policy-message" class="hidden"><span id="override-policy-link" class="link">Override</span> this policy.</div>
            <div id="add-local-admin-message" class="hidden"><span id="add-local-admin-link" class="link">Add</span> policy for additional administrators.</div>
            <div class="clear"></div>
        </div>
        <div id="global-policy-controls" class="hidden policy-controls">
            <div id="policy-type-list"></div>
            <div id="controller-policy-target" class="hidden"></div>
            <div id="restricted-component-required-permissions" class="hidden"></div>
            <div class="clear"></div>
        </div>
        <div id="component-policy-controls" class="hidden policy-controls">
            <div id="policy-selected-component-container" class="hidden policy-selected-component-container">
                <div class="policy-selected-component-type-icon">
                    <i class="icon icon-drop" ng-class="appCtrl.serviceProvider.graphControlsCtrl.getContextIcon()"></i>
                </div>
                <div class="policy-selected-component-details-container">
                    <div class="policy-selected-component-name">{{appCtrl.serviceProvider.graphControlsCtrl.getContextName()}}</div>
                    <div class="policy-selected-component-type" ng-class="appCtrl.serviceProvider.graphControlsCtrl.hide()">{{appCtrl.serviceProvider.graphControlsCtrl.getContextType()}}</div>
                </div>
                <div class="clear"></div>
            </div>
            <div id="policy-selected-template-container" class="hidden policy-selected-component-container">
                <div class="policy-selected-component-type-icon">
                    <i class="icon icon-template"></i>
                </div>
                <div class="policy-selected-component-details-container">
                    <div class="policy-selected-component-name"></div>
                    <div class="policy-selected-component-type">Template</div>
                </div>
                <div class="clear"></div>
            </div>
            <div id="policy-selected-controller-service-container" class="hidden policy-selected-component-container">
                <div class="policy-selected-component-type-icon">
                    <i class="icon icon-drop"></i>
                </div>
                <div class="policy-selected-component-details-container">
                    <div class="policy-selected-component-name"></div>
                    <div class="policy-selected-component-type">Controller Service</div>
                </div>
                <div class="clear"></div>
            </div>
            <div id="policy-selected-reporting-task-container" class="hidden policy-selected-component-container">
                <div class="policy-selected-component-type-icon">
                    <i class="icon icon-drop"></i>
                </div>
                <div class="policy-selected-component-details-container">
                    <div class="policy-selected-component-name"></div>
                    <div class="policy-selected-component-type">Reporting Task</div>
                </div>
                <div class="clear"></div>
            </div>
            <div id="selected-policy-component-id" class="hidden"></div>
            <div id="selected-policy-component-type" class="hidden"></div>
            <div id="component-policy-target"></div>
            <div class="clear"></div>
        </div>
        <button id="delete-policy-button" class="fa fa-trash policy-button" title="Delete this policy"></button>
        <button id="new-policy-user-button" class="fa fa-user-plus policy-button" title="Add users/groups to this policy"></button>
        <div class="clear"></div>
    </div>
    <div id="policy-table"></div>
    <div id="policy-refresh-container">
        <button id="policy-refresh-button" class="refresh-button pointer fa fa-refresh" title="Refresh"></button>
        <div class="last-refreshed-container">
            Last updated:&nbsp;<span id="policy-last-refreshed" class="last-refreshed"></span>
        </div>
        <div id="policy-loading-container" class="loading-container"></div>
        <div id="admin-policy-message" class="hidden">Only listing component specific administrators. Inherited administrators not shown.</div>
        <div id="restriction-message" class="hidden">Only listing restriction specific users. Users with permission "regardless of restrictions" not shown but are also allowed.</div>
        <div class="clear"></div>
    </div>
</div>