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
        <title>NiFi Summary</title>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
        <link rel="shortcut icon" href="images/nifi16.ico"/>
        <link rel="stylesheet" href="assets/reset.css/reset.css" type="text/css" />
        ${nf.summary.style.tags}
        <link rel="stylesheet" href="js/jquery/tabbs/jquery.tabbs.css?${project.version}" type="text/css" />
        <link rel="stylesheet" href="js/jquery/combo/jquery.combo.css?${project.version}" type="text/css" />
        <link rel="stylesheet" href="js/jquery/propertytable/jquery.propertytable.css?${project.version}" type="text/css" />
        <link rel="stylesheet" href="js/jquery/modal/jquery.modal.css?${project.version}" type="text/css" />
        <link rel="stylesheet" href="js/codemirror/lib/codemirror.css" type="text/css" />
        <link rel="stylesheet" href="js/codemirror/addon/hint/show-hint.css" type="text/css" />
        <link rel="stylesheet" href="js/jquery/nfeditor/jquery.nfeditor.css?${project.version}" type="text/css" />
        <link rel="stylesheet" href="js/jquery/nfeditor/languages/nfel.css?${project.version}" type="text/css" />
        <link rel="stylesheet" href="assets/qtip2/dist/jquery.qtip.min.css?" type="text/css" />
        <link rel="stylesheet" href="assets/jquery-ui-dist/jquery-ui.min.css" type="text/css" />
        <link rel="stylesheet" href="assets/slickgrid/slick.grid.css" type="text/css" />
        <link rel="stylesheet" href="css/slick-nifi-theme.css" type="text/css" />
        <link rel="stylesheet" href="fonts/flowfont/flowfont.css" type="text/css" />
        <link rel="stylesheet" href="assets/angular-material/angular-material.min.css" type="text/css" />
        <link rel="stylesheet" href="assets/font-awesome/css/font-awesome.min.css" type="text/css" />
        <script type="text/javascript" src="js/codemirror/lib/codemirror-compressed.js"></script>
        <script type="text/javascript" src="assets/d3/build/d3.min.js"></script>
        <script type="text/javascript" src="assets/jquery/dist/jquery.min.js"></script>
        <script type="text/javascript" src="assets/jquery-ui-dist/jquery-ui.min.js"></script>
        <script type="text/javascript" src="js/jquery/jquery.base64.js"></script>
        <script type="text/javascript" src="js/jquery/jquery.center.js"></script>
        <script type="text/javascript" src="js/jquery/tabbs/jquery.tabbs.js?${project.version}"></script>
        <script type="text/javascript" src="js/jquery/combo/jquery.combo.js?${project.version}"></script>
        <script type="text/javascript" src="js/jquery/modal/jquery.modal.js?${project.version}"></script>
        <script type="text/javascript" src="js/jquery/jquery.ellipsis.js"></script>
        <script type="text/javascript" src="js/jquery/jquery.each.js"></script>
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
        <script type="text/javascript" src="js/nf/nf-namespace.js?${project.version}"></script>
        <script type="text/javascript" src="js/nf/nf-ng-namespace.js?${project.version}"></script>
        ${nf.summary.script.tags}
        <script type="text/javascript" src="js/jquery/nfeditor/languages/nfel.js?${project.version}"></script>
        <script type="text/javascript" src="js/jquery/nfeditor/jquery.nfeditor.js?${project.version}"></script>
        <script type="text/javascript" src="js/jquery/propertytable/jquery.propertytable.js?${project.version}"></script>
    </head>
    <body ng-controller="ngSummaryAppCtrl">
        <jsp:include page="/WEB-INF/partials/message-pane.jsp"/>
        <jsp:include page="/WEB-INF/partials/banners-utility.jsp"/>
        <jsp:include page="/WEB-INF/partials/ok-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/processor-details.jsp"/>
        <jsp:include page="/WEB-INF/partials/connection-details.jsp"/>
        <jsp:include page="/WEB-INF/partials/status-history-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/summary/summary-content.jsp"/>
        <jsp:include page="/WEB-INF/partials/summary/cluster-processor-summary-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/summary/cluster-input-port-summary-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/summary/cluster-output-port-summary-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/summary/cluster-remote-process-group-summary-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/summary/cluster-connection-summary-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/summary/cluster-process-group-summary-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/summary/system-diagnostics-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/summary/view-single-node-dialog.jsp"/>
    </body>
</html>
