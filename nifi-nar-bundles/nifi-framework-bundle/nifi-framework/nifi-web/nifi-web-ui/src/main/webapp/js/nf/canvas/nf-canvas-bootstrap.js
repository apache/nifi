/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* global define, module, require, exports */

(function (root, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['jquery',
                'angular',
                'nf.Common',
                'nf.CanvasUtils',
                'nf.ErrorHandler',
                'nf.Client',
                'nf.ClusterSummary',
                'nf.Dialog',
                'nf.Storage',
                'nf.Canvas',
                'nf.Graph',
                'nf.ContextMenu',
                'nf.Shell',
                'nf.Settings',
                'nf.Snippet',
                'nf.Actions',
                'nf.QueueListing',
                'nf.ComponentState',
                'nf.Draggable',
                'nf.Connectable',
                'nf.StatusHistory',
                'nf.Birdseye',
                'nf.ConnectionConfiguration',
                'nf.ControllerService',
                'nf.ReportingTask',
                'nf.PolicyManagement',
                'nf.ProcessorConfiguration',
                'nf.ProcessGroupConfiguration',
                'nf.ControllerServices',
                'nf.RemoteProcessGroupConfiguration',
                'nf.RemoteProcessGroupPorts',
                'nf.PortConfiguration',
                'nf.LabelConfiguration',
                'nf.ProcessorDetails',
                'nf.PortDetails',
                'nf.ConnectionDetails',
                'nf.RemoteProcessGroupDetails',
                'nf.GoTo',
                'nf.ng.Bridge',
                'nf.ng.AppCtrl',
                'nf.ng.AppConfig',
                'nf.ng.ServiceProvider',
                'nf.ng.BreadcrumbsCtrl',
                'nf.ng.Canvas.HeaderCtrl',
                'nf.ng.Canvas.FlowStatusCtrl',
                'nf.ng.Canvas.GlobalMenuCtrl',
                'nf.ng.Canvas.ToolboxCtrl',
                'nf.ng.ProcessorComponent',
                'nf.ng.InputPortComponent',
                'nf.ng.OutputPortComponent',
                'nf.ng.GroupComponent',
                'nf.ng.RemoteProcessGroupComponent',
                'nf.ng.FunnelComponent',
                'nf.ng.TemplateComponent',
                'nf.ng.LabelComponent',
                'nf.ng.Canvas.GraphControlsCtrl',
                'nf.ng.Canvas.NavigateCtrl',
                'nf.ng.Canvas.OperateCtrl',
                'nf.ng.BreadcrumbsDirective',
                'nf.ng.DraggableDirective'],
            function ($, angular, common, canvasUtils, errorHandler, client, clusterSummary, dialog, storage, canvas, graph, contextMenu, shell, settings, actions, snippet, queueListing, componentState, draggable, connectable, statusHistory, birdseye, connectionConfiguration, controllerService, reportingTask, policyManagement, processorConfiguration, processGroupConfiguration, controllerServices, remoteProcessGroupConfiguration, remoteProcessGroupPorts, portConfiguration, labelConfiguration, processorDetails, portDetails, connectionDetails, remoteProcessGroupDetails, nfGoto, angularBridge, appCtrl, appConfig, serviceProvider, breadcrumbsCtrl, headerCtrl, flowStatusCtrl, globalMenuCtrl, toolboxCtrl, processorComponent, inputPortComponent, outputPortComponent, processGroupComponent, remoteProcessGroupComponent, funnelComponent, templateComponent, labelComponent, graphControlsCtrl, navigateCtrl, operateCtrl, breadcrumbsDirective, draggableDirective) {
                return factory($, angular, common, canvasUtils, errorHandler, client, clusterSummary, dialog, storage, canvas, graph, contextMenu, shell, settings, actions, snippet, queueListing, componentState, draggable, connectable, statusHistory, birdseye, connectionConfiguration, controllerService, reportingTask, policyManagement, processorConfiguration, processGroupConfiguration, controllerServices, remoteProcessGroupConfiguration, remoteProcessGroupPorts, portConfiguration, labelConfiguration, processorDetails, portDetails, connectionDetails, remoteProcessGroupDetails, nfGoto, angularBridge, appCtrl, appConfig, serviceProvider, breadcrumbsCtrl, headerCtrl, flowStatusCtrl, globalMenuCtrl, toolboxCtrl, processorComponent, inputPortComponent, outputPortComponent, processGroupComponent, remoteProcessGroupComponent, funnelComponent, templateComponent, labelComponent, graphControlsCtrl, navigateCtrl, operateCtrl, breadcrumbsDirective, draggableDirective);
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = factory(require('jquery'),
            require('angular'),
            require('nf.Common'),
            require('nf.CanvasUtils'),
            require('nf.ErrorHandler'),
            require('nf.Client'),
            require('nf.ClusterSummary'),
            require('nf.Dialog'),
            require('f.Storage'),
            require('nf.Canvas'),
            require('nf.Graph'),
            require('nf.ContextMenu'),
            require('nf.Shell'),
            require('nf.Settings'),
            require('nf.Actions'),
            require('nf.Snippet'),
            require('nf.QueueListing'),
            require('nf.ComponentState'),
            require('nf.Draggable'),
            require('nf.Connectable'),
            require('nf.StatusHistory'),
            require('nf.Birdseye'),
            require('nf.ConnectionConfiguration'),
            require('nf.ControllerService'),
            require('nf.ReportingTask'),
            require('nf.PolicyManagement'),
            require('nf.ProcessorConfiguration'),
            require('nf.ProcessGroupConfiguration'),
            require('nf.ControllerServices'),
            require('nf.RemoteProcessGroupConfiguration'),
            require('nf.RemoteProcessGroupPorts'),
            require('nf.PortConfiguration'),
            require('nf.LabelConfiguration'),
            require('nf.ProcessorDetails'),
            require('nf.PortDetails'),
            require('nf.ConnectionDetails'),
            require('nf.RemoteProcessGroupDetails'),
            require('nf.GoTo'),
            require('nf.ng.Bridge'),
            require('nf.ng.AppCtrl'),
            require('nf.ng.AppConfig'),
            require('nf.ng.ServiceProvider'),
            require('nf.ng.BreadcrumbsCtrl'),
            require('nf.ng.Canvas.HeaderCtrl'),
            require('nf.ng.Canvas.FlowStatusCtrl'),
            require('nf.ng.Canvas.GlobalMenuCtrl'),
            require('nf.ng.Canvas.ToolboxCtrl'),
            require('nf.ng.ProcessorComponent'),
            require('nf.ng.InputPortComponent'),
            require('nf.ng.OutputPortComponent'),
            require('nf.ng.GroupComponent'),
            require('nf.ng.RemoteProcessGroupComponent'),
            require('nf.ng.FunnelComponent'),
            require('nf.ng.TemplateComponent'),
            require('nf.ng.LabelComponent'),
            require('nf.ng.Canvas.GraphControlsCtrl'),
            require('nf.ng.Canvas.NavigateCtrl'),
            require('nf.ng.Canvas.OperateCtrl'),
            require('nf.ng.BreadcrumbsDirective'),
            require('nf.ng.DraggableDirective'));
    } else {
        factory(root.$,
            root.angular,
            root.nf.Common,
            root.nf.CanvasUtils,
            root.nf.ErrorHandler,
            root.nf.Client,
            root.nf.ClusterSummary,
            root.nf.Dialog,
            root.nf.Storage,
            root.nf.Canvas,
            root.nf.Graph,
            root.nf.ContextMenu,
            root.nf.Shell,
            root.nf.Settings,
            root.nf.Actions,
            root.nf.Snippet,
            root.nf.QueueListing,
            root.nf.ComponentState,
            root.nf.Draggable,
            root.nf.Connectable,
            root.nf.StatusHistory,
            root.nf.Birdseye,
            root.nf.ConnectionConfiguration,
            root.nf.ControllerService,
            root.nf.ReportingTask,
            root.nf.PolicyManagement,
            root.nf.ProcessorConfiguration,
            root.nf.ProcessGroupConfiguration,
            root.nf.ControllerServices,
            root.nf.RemoteProcessGroupConfiguration,
            root.nf.RemoteProcessGroupPorts,
            root.nf.PortConfiguration,
            root.nf.LabelConfiguration,
            root.nf.ProcessorDetails,
            root.nf.PortDetails,
            root.nf.ConnectionDetails,
            root.nf.RemoteProcessGroupDetails,
            root.nf.GoTo,
            root.nf.ng.Bridge,
            root.nf.ng.AppCtrl,
            root.nf.ng.AppConfig,
            root.nf.ng.ServiceProvider,
            root.nf.ng.BreadcrumbsCtrl,
            root.nf.ng.Canvas.HeaderCtrl,
            root.nf.ng.Canvas.FlowStatusCtrl,
            root.nf.ng.Canvas.GlobalMenuCtrl,
            root.nf.ng.Canvas.ToolboxCtrl,
            root.nf.ng.ProcessorComponent,
            root.nf.ng.InputPortComponent,
            root.nf.ng.OutputPortComponent,
            root.nf.ng.GroupComponent,
            root.nf.ng.RemoteProcessGroupComponent,
            root.nf.ng.FunnelComponent,
            root.nf.ng.TemplateComponent,
            root.nf.ng.LabelComponent,
            root.nf.ng.Canvas.GraphControlsCtrl,
            root.nf.ng.Canvas.NavigateCtrl,
            root.nf.ng.Canvas.OperateCtrl,
            root.nf.ng.BreadcrumbsDirective,
            root.nf.ng.DraggableDirective);
    }
}(this, function ($, angular, common, canvasUtils, errorHandler, client, clusterSummary, dialog, storage, canvas, graph, contextMenu, shell, settings, actions, snippet, queueListing, componentState, draggable, connectable, statusHistory, birdseye, connectionConfiguration, controllerService, reportingTask, policyManagement, processorConfiguration, processGroupConfiguration, controllerServices, remoteProcessGroupConfiguration, remoteProcessGroupPorts, portConfiguration, labelConfiguration, processorDetails, portDetails, connectionDetails, remoteProcessGroupDetails, nfGoto, angularBridge, appCtrl, appConfig, serviceProvider, breadcrumbsCtrl, headerCtrl, flowStatusCtrl, globalMenuCtrl, toolboxCtrl, processorComponent, inputPortComponent, outputPortComponent, processGroupComponent, remoteProcessGroupComponent, funnelComponent, templateComponent, labelComponent, graphControlsCtrl, navigateCtrl, operateCtrl, breadcrumbsDirective, draggableDirective) {

    var config = {
        urls: {
            flowConfig: '../nifi-api/flow/config'
        }
    };

    /**
     * Bootstrap the canvas application.
     */
    $(document).ready(function () {
        if (canvas.SUPPORTS_SVG) {

            //Create Angular App
            var app = angular.module('ngCanvasApp', ['ngResource', 'ngRoute', 'ngMaterial', 'ngMessages']);

            //Define Dependency Injection Annotations
            appConfig.$inject = ['$mdThemingProvider', '$compileProvider'];
            appCtrl.$inject = ['$scope', 'serviceProvider', '$compile', 'headerCtrl', 'graphControlsCtrl'];
            serviceProvider.$inject = [];
            breadcrumbsCtrl.$inject = ['serviceProvider'];
            headerCtrl.$inject = ['serviceProvider', 'toolboxCtrl', 'globalMenuCtrl', 'flowStatusCtrl'];
            flowStatusCtrl.$inject = ['serviceProvider'];
            globalMenuCtrl.$inject = ['serviceProvider'];
            toolboxCtrl.$inject = ['processorComponent',
                'inputPortComponent',
                'outputPortComponent',
                'groupComponent',
                'remoteGroupComponent',
                'funnelComponent',
                'templateComponent',
                'labelComponent'];
            processorComponent.$inject = ['serviceProvider'];
            inputPortComponent.$inject = ['serviceProvider'];
            outputPortComponent.$inject = ['serviceProvider'];
            processGroupComponent.$inject = ['serviceProvider'];
            remoteProcessGroupComponent.$inject = ['serviceProvider'];
            funnelComponent.$inject = ['serviceProvider'];
            templateComponent.$inject = ['serviceProvider'];
            labelComponent.$inject = ['serviceProvider'];
            graphControlsCtrl.$inject = ['serviceProvider', 'navigateCtrl', 'operateCtrl'];
            navigateCtrl.$inject = [];
            operateCtrl.$inject = [];
            breadcrumbsDirective.$inject = ['breadcrumbsCtrl'];
            draggableDirective.$inject = [];

            //Configure Angular App
            app.config(appConfig);

            //Define Angular App Controllers
            app.controller('ngCanvasAppCtrl', appCtrl);

            //Define Angular App Services
            app.service('serviceProvider', serviceProvider);
            app.service('breadcrumbsCtrl', breadcrumbsCtrl);
            app.service('headerCtrl', headerCtrl);
            app.service('flowStatusCtrl', flowStatusCtrl);
            app.service('globalMenuCtrl', globalMenuCtrl);
            app.service('toolboxCtrl', toolboxCtrl);
            app.service('processorComponent', processorComponent);
            app.service('inputPortComponent', inputPortComponent);
            app.service('outputPortComponent', outputPortComponent);
            app.service('groupComponent', processGroupComponent);
            app.service('remoteGroupComponent', remoteProcessGroupComponent);
            app.service('funnelComponent', funnelComponent);
            app.service('templateComponent', templateComponent);
            app.service('labelComponent', labelComponent);
            app.service('graphControlsCtrl', graphControlsCtrl);
            app.service('navigateCtrl', navigateCtrl);
            app.service('operateCtrl', operateCtrl);

            //Define Angular App Directives
            app.directive('nfBreadcrumbs', breadcrumbsDirective);
            app.directive('nfDraggable', draggableDirective);

            // initialize the canvas utils and invert control of the canvas,
            // actions, snippet, birdseye, and graph
            canvasUtils.init(canvas, actions, snippet, birdseye, graph);

            //Manually Boostrap Angular App
            angularBridge.injector = angular.bootstrap($('body'), ['ngCanvasApp'], {strictDi: true});

            // initialize the NiFi
            var userXhr = canvas.init();

            userXhr.done(function () {
                // load the client id
                var clientXhr = client.init();

                // get the controller config to register the status poller
                var configXhr = $.ajax({
                    type: 'GET',
                    url: config.urls.flowConfig,
                    dataType: 'json'
                });

                // ensure the config requests are loaded
                $.when(configXhr, clusterSummary.loadClusterSummary(), userXhr, clientXhr).done(function (configResult) {
                    var configResponse = configResult[0];

                    // calculate the canvas offset
                    var canvasContainer = $('#canvas-container');
                    canvas.CANVAS_OFFSET = canvasContainer.offset().top;

                    // get the config details
                    var configDetails = configResponse.flowConfiguration;

                    // show disconnected message on load if necessary
                    if (clusterSummary.isClustered() && !clusterSummary.isConnectedToCluster()) {
                        dialog.showDisconnectedFromClusterMessage();
                    }

                    // get the auto refresh interval
                    var autoRefreshIntervalSeconds = parseInt(configDetails.autoRefreshIntervalSeconds, 10);

                    // record whether we can configure the authorizer
                    canvas.setConfigurableAuthorizer(configDetails.supportsConfigurableAuthorizer);

                    // init storage
                    storage.init();

                    // initialize the application
                    canvas.initCanvas();
                    canvas.View.init();
                    // initialize the context menu and invert control of the actions
                    contextMenu.init(actions);
                    // initialize the shell and invert control of the context menu
                    shell.init(contextMenu);
                    angularBridge.injector.get('headerCtrl').init();
                    settings.init();
                    actions.init();
                    queueListing.init();
                    componentState.init();

                    // initialize the component behaviors
                    draggable.init();
                    connectable.init();

                    // initialize the chart
                    statusHistory.init(configDetails.timeOffset);

                    // initialize the birdseye
                    birdseye.init(graph);

                    // initialize the connection config and invert control of the birdseye and graph
                    connectionConfiguration.init(birdseye, graph);
                    controllerService.init();
                    reportingTask.init(settings);
                    policyManagement.init();
                    processorConfiguration.init();
                    // initialize the PG config and invert control of the controllerServices
                    processGroupConfiguration.init(controllerServices);
                    remoteProcessGroupConfiguration.init();
                    remoteProcessGroupPorts.init();
                    portConfiguration.init();
                    labelConfiguration.init();
                    processorDetails.init(true);
                    portDetails.init();
                    connectionDetails.init();
                    remoteProcessGroupDetails.init();
                    nfGoto.init();
                    graph.init().done(function () {
                        angularBridge.injector.get('graphControlsCtrl').init();

                        // determine the split between the polling
                        var pollingSplit = autoRefreshIntervalSeconds / 2;

                        // register the polling
                        setTimeout(function () {
                            canvas.startPolling(autoRefreshIntervalSeconds);
                        }, pollingSplit * 1000);

                        // hide the splash screen
                        canvas.hideSplash();
                    }).fail(errorHandler.handleAjaxError);
                }).fail(errorHandler.handleAjaxError);
            }).fail(errorHandler.handleAjaxError);

            //initialize toolbox components tooltips
            $('.component-button').qtip($.extend({}, common.config.tooltipConfig));
        } else {
            $('#message-title').text('Unsupported Browser');
            $('#message-content').text('Flow graphs are shown using SVG. Please use a browser that supports rendering SVG.');

            // show the error pane
            $('#message-pane').show();

            // hide the splash screen
            canvas.hideSplash();
        }
    });
}));
