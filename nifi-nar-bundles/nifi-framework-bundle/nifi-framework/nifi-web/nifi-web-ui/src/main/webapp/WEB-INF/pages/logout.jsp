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
        <title>NiFi Logout</title>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
        <link rel="shortcut icon" href="images/nifi16.ico"/>
        <link rel="stylesheet" href="assets/reset.css/reset.css" type="text/css" />
        <link rel="stylesheet" href="fonts/flowfont/flowfont.css" type="text/css" />
        ${nf.logout.style.tags}
        <link rel="stylesheet" href="js/jquery/modal/jquery.modal.css?${project.version}" type="text/css" />
        <link rel="stylesheet" href="assets/qtip2/dist/jquery.qtip.min.css?" type="text/css" />
        <link rel="stylesheet" href="assets/jquery-ui-dist/jquery-ui.min.css" type="text/css" />
        <link rel="stylesheet" href="fonts/flowfont/flowfont.css" type="text/css" />
        <script type="text/javascript" src="assets/jquery/dist/jquery.min.js"></script>
        <script type="text/javascript" src="js/jquery/jquery.base64.js"></script>
        <script type="text/javascript" src="js/jquery/jquery.count.js"></script>
        <script type="text/javascript" src="js/jquery/jquery.center.js"></script>
        <script type="text/javascript" src="js/jquery/modal/jquery.modal.js?${project.version}"></script>
        <script type="text/javascript" src="assets/qtip2/dist/jquery.qtip.min.js"></script>
        <script type="text/javascript" src="assets/jquery-ui-dist/jquery-ui.min.js"></script>
        <script type="text/javascript" src="js/nf/nf-namespace.js?${project.version}"></script>
        <script type="text/javascript" src="assets/lodash-core/distrib/lodash-core.min.js"></script>
        ${nf.logout.script.tags}
    </head>
    <body class="logout-body">
    <div id="logout-user-links-container">
        <ul id="logout-user-links" class="links">
            <li>
                <span id="user-home" class="link">home</span>
            </li>
        </ul>
    </div>
        <div id="logout-contents-container">
            <jsp:include page="/WEB-INF/partials/logout/logout-message.jsp"/>
        </div>
    </body>
</html>