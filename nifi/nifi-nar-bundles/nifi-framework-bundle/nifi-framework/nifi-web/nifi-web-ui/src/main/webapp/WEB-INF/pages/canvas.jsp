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
        <link rel="stylesheet" href="css/reset.css" type="text/css" />
        ${nf.canvas.style.tags}
        <link rel="stylesheet" href="js/codemirror/lib/codemirror.css" type="text/css" />
        <link rel="stylesheet" href="js/codemirror/addon/hint/show-hint.css" type="text/css" />
        <link rel="stylesheet" href="js/jquery/nfeditor/jquery.nfeditor.css?${project.version}" type="text/css" />
        <link rel="stylesheet" href="js/jquery/nfeditor/languages/nfel.css?${project.version}" type="text/css" />
        <link rel="stylesheet" href="js/jquery/tabbs/jquery.tabbs.css?${project.version}" type="text/css" />
        <link rel="stylesheet" href="js/jquery/combo/jquery.combo.css?${project.version}" type="text/css" />
        <link rel="stylesheet" href="js/jquery/modal/jquery.modal.css?${project.version}" type="text/css" />
        <link rel="stylesheet" href="js/jquery/qtip2/jquery.qtip.min.css?" type="text/css" />
        <link rel="stylesheet" href="js/jquery/ui-smoothness/jquery-ui-1.10.4.min.css" type="text/css" />
        <link rel="stylesheet" href="js/jquery/minicolors/jquery.minicolors.css" type="text/css" />
        <link rel="stylesheet" href="js/jquery/slickgrid/css/slick.grid.css" type="text/css" />
        <link rel="stylesheet" href="js/jquery/slickgrid/css/slick-default-theme.css" type="text/css" />
        <script type="text/javascript" src="js/codemirror/lib/codemirror-compressed.js"></script>
        <script type="text/javascript" src="js/jquery/jquery-2.1.1.min.js"></script>
        <script type="text/javascript" src="js/jquery/ui-smoothness/jquery-ui-1.10.4.min.js"></script>
        <script type="text/javascript" src="js/jquery/jquery.center.js"></script>
        <script type="text/javascript" src="js/jquery/jquery.count.js"></script>
        <script type="text/javascript" src="js/jquery/jquery.ellipsis.js"></script>
        <script type="text/javascript" src="js/jquery/jquery.each.js"></script>
        <script type="text/javascript" src="js/jquery/jquery.tab.js"></script>
        <script type="text/javascript" src="js/jquery/tabbs/jquery.tabbs.js?${project.version}"></script>
        <script type="text/javascript" src="js/jquery/combo/jquery.combo.js?${project.version}"></script>
        <script type="text/javascript" src="js/jquery/modal/jquery.modal.js?${project.version}"></script>
        <script type="text/javascript" src="js/jquery/minicolors/jquery.minicolors.min.js"></script>
        <script type="text/javascript" src="js/jquery/qtip2/jquery.qtip.min.js"></script>
        <script type="text/javascript" src="js/jquery/jquery.event.drag-2.2.min.js"></script>
        <script type="text/javascript" src="js/jquery/slickgrid/plugins/slick.cellrangeselector.js"></script>
        <script type="text/javascript" src="js/jquery/slickgrid/plugins/slick.cellselectionmodel.js"></script>
        <script type="text/javascript" src="js/jquery/slickgrid/plugins/slick.rowselectionmodel.js"></script>
        <script type="text/javascript" src="js/jquery/slickgrid/plugins/slick.autotooltips.js"></script>
        <script type="text/javascript" src="js/jquery/slickgrid/slick.formatters.js"></script>
        <script type="text/javascript" src="js/jquery/slickgrid/slick.editors.js"></script>
        <script type="text/javascript" src="js/jquery/slickgrid/slick.dataview.js"></script>
        <script type="text/javascript" src="js/jquery/slickgrid/slick.core.js"></script>
        <script type="text/javascript" src="js/jquery/slickgrid/slick.grid.js"></script>
        <script type="text/javascript" src="js/json2.js"></script>
        <script type="text/javascript" src="js/nf/nf-namespace.js?${project.version}"></script>
        ${nf.canvas.script.tags}
        <script type="text/javascript" src="js/jquery/nfeditor/languages/nfel.js?${project.version}"></script>
        <script type="text/javascript" src="js/jquery/nfeditor/jquery.nfeditor.js?${project.version}"></script>
    </head>
    <body id="canvas-body">
        <div id="splash">
            <img id="splash-img" src="images/loadAnimation.gif" alt="Loading..."/>
        </div>
        <jsp:include page="/WEB-INF/partials/message-pane.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/registration.jsp"/>
        <jsp:include page="/WEB-INF/partials/banners-main.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/canvas-header.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/about-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/ok-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/yes-no-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/status-history-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/new-processor-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/new-port-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/new-process-group-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/new-remote-process-group-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/new-template-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/instantiate-template-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/new-processor-property-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/fill-color-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/connections-dialog.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/flow-status.jsp"/>
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
        <jsp:include page="/WEB-INF/partials/canvas/processor-configuration.jsp"/>
        <jsp:include page="/WEB-INF/partials/processor-details.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/process-group-configuration.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/process-group-details.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/remote-process-group-configuration.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/remote-process-group-details.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/remote-process-group-ports.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/remote-port-configuration.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/port-configuration.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/port-details.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/secure-port-configuration.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/secure-port-details.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/label-configuration.jsp"/>
        <jsp:include page="/WEB-INF/partials/canvas/connection-configuration.jsp"/>
        <jsp:include page="/WEB-INF/partials/connection-details.jsp"/>
        <div id="faded-background"></div>
        <div id="glass-pane"></div>
        <div id="context-menu" class="unselectable"></div>
    </body>
</html>