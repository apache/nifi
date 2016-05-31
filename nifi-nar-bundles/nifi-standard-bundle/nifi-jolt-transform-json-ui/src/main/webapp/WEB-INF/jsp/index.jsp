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
<html lang="en">
<head>
    <link rel="stylesheet" type="text/css" href="../nifi/js/codemirror/lib/codemirror.css"/>
    <link rel="stylesheet" type="text/css" href="../nifi/js/codemirror/addon/lint/lint.css">
    <link rel="stylesheet" type="text/css" href="js/angular/angular-material/angular-material.css">
    <link rel="stylesheet" type="text/css" href="css/main.css">
</head>

<body ng-app="standardUI" ng-cloak>

<!--Parent Libraries-->
<script type="text/javascript" src="../nifi/js/codemirror/lib/codemirror-compressed.js"></script>
<script type="text/javascript" src="../nifi/js/codemirror/addon/lint/lint.js"></script>
<script type="text/javascript" src="../nifi/js/codemirror/addon/lint/json-lint.js"></script>
<script type="text/javascript" src="../nifi/js/jsonlint/jsonlint.min.js"></script>

<!--Local Libraries-->
<script type="text/javascript" src="js/angular/angular.min.js"></script>
<script type="text/javascript" src="js/angular/angular-animate/angular-animate.min.js"></script>
<script type="text/javascript" src="js/angular/angular-aria/angular-aria.min.js"></script>
<script type="text/javascript" src="js/angular/angular-messages/angular-messages.min.js"></script>
<script type="text/javascript" src="js/angular/angular-material/angular-material.min.js"></script>
<script type="text/javascript" src="js/angular-ui/angular-ui-codemirror/ui-codemirror.min.js"></script>
<script type="text/javascript" src="js/angular-ui/angular-ui-router/angular-ui-router.min.js"></script>
<script type="text/javascript" src="js/js-beautify/beautify.js"></script>

<!--StandardUI-->

<script type="text/javascript" src="app/app.js"></script>

<!--Global Components-->
<script type="text/javascript" src="app/components/error/error.state.js"></script>
<script type="text/javascript" src="app/components/processor/processor.service.js"></script>

<!--View Components-->
<script type="text/javascript" src="app/main/main.state.js"></script>
<script type="text/javascript" src="app/main/main.controller.js"></script>

<!--Transform Json Advanced UI-->
<script type="text/javascript" src="app/transformjson/transformjson.state.js"></script>
<script type="text/javascript" src="app/transformjson/transformjson.controller.js"></script>
<script type="text/javascript" src="app/transformjson/transformjson.service.js"></script>


<div ui-view>

</div>

</body>
</html>
