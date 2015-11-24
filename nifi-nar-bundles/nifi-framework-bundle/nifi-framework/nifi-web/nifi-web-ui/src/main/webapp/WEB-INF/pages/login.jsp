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
        <title>NiFi Login</title>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
        <link rel="shortcut icon" href="images/nifi16.ico"/>
        <link rel="stylesheet" href="css/reset.css" type="text/css" />
        ${nf.login.style.tags}
        <link rel="stylesheet" href="js/jquery/modal/jquery.modal.css?${project.version}" type="text/css" />
        <link rel="stylesheet" href="js/jquery/qtip2/jquery.qtip.min.css?" type="text/css" />
        <link rel="stylesheet" href="js/jquery/ui-smoothness/jquery-ui-1.10.4.min.css" type="text/css" />
        <script type="text/javascript" src="js/jquery/jquery-2.1.1.min.js"></script>
        <script type="text/javascript" src="js/jquery/jquery.base64.js"></script>
        <script type="text/javascript" src="js/jquery/jquery.count.js"></script>
        <script type="text/javascript" src="js/jquery/jquery.center.js"></script>
        <script type="text/javascript" src="js/jquery/modal/jquery.modal.js?${project.version}"></script>
        <script type="text/javascript" src="js/jquery/qtip2/jquery.qtip.min.js"></script>
        <script type="text/javascript" src="js/jquery/ui-smoothness/jquery-ui-1.10.4.min.js"></script>
        <script type="text/javascript" src="js/nf/nf-namespace.js?${project.version}"></script>
        ${nf.login.script.tags}
    </head>
    <body class="login-body">
        <div id="login-user-links-container">
            <ul id="login-user-links" class="links">
                <li id="user-logout-container" style="display: none;">
                    <span id="user-logout" class="link">logout</span>
                </li>
                <li>
                    <span id="user-home" class="link">home</span>
                </li>
            </ul>
            <div class="clear"></div>
        </div>
        <div id="login-contents-container">
            <jsp:include page="/WEB-INF/partials/login/login-message.jsp"/>
            <jsp:include page="/WEB-INF/partials/login/login-form.jsp"/>
            <jsp:include page="/WEB-INF/partials/login/nifi-registration-form.jsp"/>
            <jsp:include page="/WEB-INF/partials/login/login-submission.jsp"/>
            <jsp:include page="/WEB-INF/partials/login/login-progress.jsp"/>
        </div>
        <jsp:include page="/WEB-INF/partials/ok-dialog.jsp"/>
        <div id="faded-background"></div>
        <div id="glass-pane"></div>
    </body>
</html>