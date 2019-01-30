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
<!DOCTYPE html>
<html>
    <head>
        <title>NiFi</title>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
        <link rel="shortcut icon" href="images/nifi16.ico"/>
        <link rel="stylesheet" href="assets/reset.css/reset.css" type="text/css" />
        ${nf.canvas.style.tags}
        <link rel="stylesheet" href="js/codemirror/lib/codemirror.css" type="text/css" />
        <link rel="stylesheet" href="js/codemirror/addon/hint/show-hint.css" type="text/css" />
        <link rel="stylesheet" href="js/jquery/nfeditor/jquery.nfeditor.css?${project.version}" type="text/css" />
        <link rel="stylesheet" href="js/jquery/nfeditor/languages/nfel.css?${project.version}" type="text/css" />
        <link rel="stylesheet" href="js/jquery/tabbs/jquery.tabbs.css?${project.version}" type="text/css" />
        <link rel="stylesheet" href="js/jquery/combo/jquery.combo.css?${project.version}" type="text/css" />
        <link rel="stylesheet" href="js/jquery/propertytable/jquery.propertytable.css?${project.version}" type="text/css" />
        <link rel="stylesheet" href="js/jquery/tagcloud/jquery.tagcloud.css?${project.version}" type="text/css" />
        <link rel="stylesheet" href="js/jquery/modal/jquery.modal.css?${project.version}" type="text/css" />
        <link rel="stylesheet" href="js/jquery/statusbar/jquery.statusbar.css?${project.version}" type="text/css" />
        <link rel="stylesheet" href="assets/qtip2/dist/jquery.qtip.min.css?" type="text/css" />
        <link rel="stylesheet" href="assets/jquery-ui-dist/jquery-ui.min.css" type="text/css" />
        <link rel="stylesheet" href="assets/jquery-minicolors/jquery.minicolors.css" type="text/css" />
        <link rel="stylesheet" href="assets/slickgrid/slick.grid.css" type="text/css" />
        <link rel="stylesheet" href="css/slick-nifi-theme.css" type="text/css" />
        <link rel="stylesheet" href="fonts/flowfont/flowfont.css" type="text/css" />
        <link rel="stylesheet" href="assets/angular-material/angular-material.min.css" type="text/css" />
        <link rel="stylesheet" href="assets/font-awesome/css/font-awesome.min.css" type="text/css" />
        <script>
            //force browsers to use URLSearchParams polyfill do to bugs and inconsistent browser implementations
            URLSearchParams = undefined;
        </script>
        <script type="text/javascript" src="assets/url-search-params/build/url-search-params.js"></script>
        <script type="text/javascript" src="js/codemirror/lib/codemirror-compressed.js"></script>
        <script type="text/javascript" src="assets/d3/build/d3.min.js"></script>
        <script type="text/javascript" src="assets/d3-selection-multi/build/d3-selection-multi.min.js"></script>
        <script type="text/javascript" src="assets/jquery/dist/jquery.min.js"></script>
        <script type="text/javascript" src="assets/jquery-ui-dist/jquery-ui.min.js"></script>
        <script type="text/javascript" src="js/jquery/jquery.base64.js"></script>
        <script type="text/javascript" src="js/jquery/jquery.center.js"></script>
        <script type="text/javascript" src="js/jquery/jquery.ellipsis.js"></script>
        <script type="text/javascript" src="js/jquery/jquery.each.js"></script>
        <script type="text/javascript" src="js/jquery/jquery.tab.js"></script>
        <script type="text/javascript" src="assets/jquery-form/jquery.form.js"></script>
        <script type="text/javascript" src="js/jquery/tabbs/jquery.tabbs.js?${project.version}"></script>
        <script type="text/javascript" src="js/jquery/combo/jquery.combo.js?${project.version}"></script>
        <script type="text/javascript" src="js/jquery/modal/jquery.modal.js?${project.version}"></script>
        <script type="text/javascript" src="assets/jquery-minicolors/jquery.minicolors.min.js"></script>
        <script type="text/javascript" src="assets/lodash-core/distrib/lodash-core.min.js"></script>
        <script type="text/javascript" src="assets/qtip2/dist/jquery.qtip.min.js"></script>
        <script type="text/javascript" src="assets/slickgrid/lib/jquery.event.drag-2.3.0.js"></script>
        <script type="text/javascript" src="assets/slickgrid/plugins/slick.cellrangeselector.js"></script>
        <script type="text/javascript" src="assets/slickgrid/plugins/slick.cellselectionmodel.js"></script>
        <script type="text/javascript" src="assets/slickgrid/plugins/slick.rowselectionmodel.js"></script>
        <script type="text/javascript" src="assets/slickgrid/plugins/slick.autotooltips.js"></script>
        <script type="text/javascript" src="assets/slickgrid/slick.formatters.js"></script>
        <script type="text/javascript" src="assets/slickgrid/slick.editors.js"></script>
        <script type="text/javascript" src="assets/slickgrid/slick.dataview.js"></script>
        <script type="text/javascript" src="assets/slickgrid/slick.core.js"></script>
        <script type="text/javascript" src="assets/slickgrid/slick.grid.js"></script>
        <script type="text/javascript" src="assets/angular/angular.min.js"></script>
        <script type="text/javascript" src="assets/angular-messages/angular-messages.min.js"></script>
        <script type="text/javascript" src="assets/angular-resource/angular-resource.min.js"></script>
        <script type="text/javascript" src="assets/angular-route/angular-route.min.js"></script>
        <script type="text/javascript" src="assets/angular-aria/angular-aria.min.js"></script>
        <script type="text/javascript" src="assets/angular-animate/angular-animate.min.js"></script>
        <script type="text/javascript" src="assets/angular-material/angular-material.min.js"></script>
        <script type="text/javascript" src="assets/JSON2/json2.js"></script>
        <script type="text/javascript" src="js/nf/nf-namespace.js?${project.version}"></script>
        <script type="text/javascript" src="js/nf/nf-ng-namespace.js?${project.version}"></script>
        <script type="text/javascript" src="js/nf/canvas/nf-ng-canvas-namespace.js?${project.version}"></script>
        ${nf.canvas.script.tags}
        <script type="text/javascript" src="js/jquery/nfeditor/languages/nfel.js?${project.version}"></script>
        <script type="text/javascript" src="js/jquery/nfeditor/jquery.nfeditor.js?${project.version}"></script>
        <script type="text/javascript" src="js/jquery/propertytable/jquery.propertytable.js?${project.version}"></script>
        <script type="text/javascript" src="js/jquery/tagcloud/jquery.tagcloud.js?${project.version}"></script>
        <script type="text/javascript" src="js/jquery/statusbar/jquery.statusbar.js?${project.version}"></script>
    </head>
    <body ng-controller="ngCanvasAppCtrl" id="canvas-body">
        <div id="splash">
            <div id="splash-img" layout="row" layout-align="center center">
                <md-progress-circular md-mode="indeterminate" class="md-warn" md-diameter="150"></md-progress-circular>
            </div>
        </div>
        <jsp:include page="/WEB-INF/partials/message-pane.jsp"/>
        <jsp:include page="/WEB-INF/partials/banners-main.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/canvas-header.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/flow-status.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/about-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/ok-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/yes-no-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/status-history-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/search-users-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/disable-controller-service-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/enable-controller-service-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/new-controller-service-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/new-reporting-task-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/new-processor-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/new-port-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/new-process-group-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/new-remote-process-group-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/new-template-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/upload-template-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/instantiate-template-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/fill-color-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/connections-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/save-flow-version-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/import-flow-version-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/revert-local-changes-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/show-local-changes-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/registry-configuration-dialog.jsp"/>
        <div id="canvas-container" class="unselectable"></div>
        <div id="canvas-tooltips">
            <div id="processor-tooltips"></div>
            <div id="port-tooltips"></div>
            <div id="process-group-tooltips"></div>
            <div id="remote-process-group-tooltips"></div>
        </div>
        <jsp:include page="/WEB-INF/partials/canvas/navigation.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/settings-content.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/shell.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/controller-service-configuration.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/reporting-task-configuration.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/processor-configuration.jsp"/>
        <jsp:include page="/WEB-INF/partials/processor-details.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/variable-configuration.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/process-group-configuration.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/override-policy-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/policy-management.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/remote-process-group-configuration.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/remote-process-group-details.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/remote-process-group-ports.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/remote-port-configuration.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/port-configuration.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/port-details.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/label-configuration.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/connection-configuration.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/drop-request-status-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/flowfile-details-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/listing-request-status-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/queue-listing.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/component-state-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/component-version-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/connection-details.jsp"/>
        <div id="context-menu" class="context-menu unselectable"></div>
        <span id="nifi-content-viewer-url" class="hidden"></span>
    </body>
</html>