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

/* global nf, define, module, require, exports */

(function (root, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['jquery',
                'angular',
                'nf.Canvas',
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
                'nf.ng.DraggableDirective',
                'nf.ng.Bridge',
                'nf.Common'],
            function ($,
                      angular,
                      canvas,
                      appCtrl,
                      config,
                      serviceProvider,
                      breadcrumbsCtrl,
                      headerCtrl,
                      flowStatusCtrl,
                      globalMenuCtrl,
                      toolboxCtrl,
                      processorComponent,
                      inputPortComponent,
                      outputPortComponent,
                      processGroupComponent,
                      remoteProcessGroupComponent,
                      funnelComponent,
                      templateComponent,
                      labelComponent,
                      graphControlsCtrl,
                      navigateCtrl,
                      operateCtrl,
                      breadcrumbsDirective,
                      draggableDirective,
                      angularBridge,
                      common) {
                return factory($,
                    angular,
                    canvas,
                    appCtrl,
                    config,
                    serviceProvider,
                    breadcrumbsCtrl,
                    headerCtrl,
                    flowStatusCtrl,
                    globalMenuCtrl,
                    toolboxCtrl,
                    processorComponent,
                    inputPortComponent,
                    outputPortComponent,
                    processGroupComponent,
                    remoteProcessGroupComponent,
                    funnelComponent,
                    templateComponent,
                    labelComponent,
                    graphControlsCtrl,
                    navigateCtrl,
                    operateCtrl,
                    breadcrumbsDirective,
                    draggableDirective,
                    angularBridge,
                    common);
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = factory(require('jquery'),
            require('angular'),
            require('nf.Canvas'),
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
            require('nf.ng.DraggableDirective'),
            require('nf.ng.Bridge'),
            require('nf.Common'));
    } else {
        factory(root.$,
            root.angular,
            root.nf.Canvas,
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
            root.nf.ng.DraggableDirective,
            root.nf.ng.Bridge,
            root.nf.Common);
    }
}(this, function ($,
                  angular,
                  canvas,
                  appCtrl,
                  config,
                  serviceProvider,
                  breadcrumbsCtrl,
                  headerCtrl,
                  flowStatusCtrl,
                  globalMenuCtrl,
                  toolboxCtrl,
                  processorComponent,
                  inputPortComponent,
                  outputPortComponent,
                  processGroupComponent,
                  remoteProcessGroupComponent,
                  funnelComponent,
                  templateComponent,
                  labelComponent,
                  graphControlsCtrl,
                  navigateCtrl,
                  operateCtrl,
                  breadcrumbsDirective,
                  draggableDirective,
                  angularBridge,
                  common) {
    /**
     * Bootstrap the canvas.
     */
    $(document).ready(function () {
        if (canvas.SUPPORTS_SVG) {

            //Create Angular App
            var app = angular.module('ngCanvasApp', ['ngResource', 'ngRoute', 'ngMaterial', 'ngMessages']);

            //Define Dependency Injection Annotations
            config.$inject = ['$mdThemingProvider', '$compileProvider'];
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
            app.config(config);

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

            //Manually Boostrap Angular App
            angularBridge.injector = angular.bootstrap($('body'), ['ngCanvasApp'], {strictDi: true});

            // initialize the NiFi
            canvas.init();

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
