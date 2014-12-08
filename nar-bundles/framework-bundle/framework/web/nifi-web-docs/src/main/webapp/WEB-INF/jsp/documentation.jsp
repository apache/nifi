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
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml">
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
        <title>NiFi Documentation</title>
        <script type="text/javascript" src="../nifi/js/jquery/jquery-1.7.min.js"></script>
        <script type="text/javascript" src="js/application.js"></script>
        <link href="css/main.css" rel="stylesheet" type="text/css" />
        <link href="css/component-usage.css" rel="stylesheet" type="text/css" />
    </head>
    <body>
        <div id="banner-header" class="main-banner-header"></div>
        <div id="banner-footer" class="main-banner-footer"></div>
        <span id="initial-selection" style="display: none;">${param.select}</span>
        <div id="documentation-header" class="documentation-header">
            <div id="nf-title">NiFi Documentation</div>
            <div id="nf-version"></div>
            <div id="selected-component"></div>
            <div class="clear"></div>
        </div>
        <div id="component-listing" class="component-listing">
            <div class="section">
                <div class="header">Documents</div>
                <div class="component-links">
                    <ul>
                    </ul>
                </div>
            </div>
            <div class="section">
                <div class="header">Processors</div>
                <div id="processor-links" class="component-links">
                    <c:choose>
                        <c:when test="${not empty processors}">
                            <c:forEach var="entry" items="${processors}">
                                <ul>
                                    <li class="component-item"><a class="component-link" href="components/${entry.value}/index.html" target="component-usage">${entry.key}</a></li>
                                </ul>
                            </c:forEach>
                            <span class="no-matching no-components hidden">No matching processors</span>
                        </c:when>
                        <c:otherwise>
                            <span class="no-components">No processor documentation found</span>
                        </c:otherwise>
                    </c:choose>
                </div>
            </div>
            <div class="section">
                <div class="header">Controller Services</div>
                <div id="controller-service-links" class="component-links">
                    <c:choose>
                        <c:when test="${not empty controllerServices}">
                            <c:forEach var="entry" items="${controllerServices}">
                                <ul>
                                    <li class="component-item"><a class="component-link" href="components/${entry.value}/index.html" target="component-usage">${entry.key}</a></li>
                                </ul>
                            </c:forEach>
                            <span class="no-matching no-components hidden">No matching controller services</span>
                        </c:when>
                        <c:otherwise>
                            <span class="no-components">No controller service documentation found</span>
                        </c:otherwise>
                    </c:choose>
                </div>
            </div>
            <div class="section">
                <div class="header">Reporting Tasks</div>
                <div id="reporting-task-links" class="component-links">
                    <c:choose>
                        <c:when test="${not empty reportingTasks}">
                            <c:forEach var="entry" items="${reportingTasks}">
                                <ul>
                                    <li class="component-item"><a class="component-link" href="components/${entry.value}/index.html" target="component-usage">${entry.key}</a></li>
                                </ul>
                            </c:forEach>
                            <span class="no-matching no-components hidden">No matching reporting tasks</span>
                        </c:when>
                        <c:otherwise>
                            <span class="no-components">No reporting task documentation found</span>
                        </c:otherwise>
                    </c:choose>
                </div>
            </div>
        </div>
        <div id="component-filter-controls">
            <div id="component-filter-container">
                <input type="text" id="component-filter"/>
            </div>
            <div id="component-filter-stats">
                Displaying&nbsp;<span id="displayed-components">${totalComponents}</span>&nbsp;of&nbsp;${totalComponents}
            </div>
        </div>
        <div id="component-usage-container">
            <iframe id="component-usage" name="component-usage" frameborder="0" class="component-usage"></iframe>
        </div>
    </body>
</html>
