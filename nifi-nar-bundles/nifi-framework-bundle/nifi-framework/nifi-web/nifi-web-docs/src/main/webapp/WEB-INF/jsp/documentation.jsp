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
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
        <link rel="shortcut icon" href="../nifi/images/nifi16.ico"/>
        <title>NiFi Documentation</title>
        <script type="text/javascript" src="../nifi/assets/jquery/dist/jquery.min.js"></script>
        <script type="text/javascript" src="js/application.js"></script>
        <link href="css/main.css" rel="stylesheet" type="text/css" />
        <link href="css/component-usage.css" rel="stylesheet" type="text/css" />
    </head>
    <body id="documentation-body">
        <div id="banner-header" class="main-banner-header"></div>
        <span id="initial-selection-type" style="display: none;">
            <%= request.getParameter("select") == null ? "" : org.apache.nifi.util.EscapeUtils.escapeHtml(request.getParameter("select")) %>
        </span>
        <span id="initial-selection-bundle-group" style="display: none;">
            <%= request.getParameter("group") == null ? "" : org.apache.nifi.util.EscapeUtils.escapeHtml(request.getParameter("group")) %>
        </span>
        <span id="initial-selection-bundle-artifact" style="display: none;">
            <%= request.getParameter("artifact") == null ? "" : org.apache.nifi.util.EscapeUtils.escapeHtml(request.getParameter("artifact")) %>
        </span>
        <span id="initial-selection-bundle-version" style="display: none;">
            <%= request.getParameter("version") == null ? "" : org.apache.nifi.util.EscapeUtils.escapeHtml(request.getParameter("version")) %>
        </span>
        <div id="documentation-header" class="documentation-header">
            <div id="component-list-toggle-link">-</div>
            <div id="header-contents">
                <div id="nf-title">NiFi Documentation</div>
                <div id="nf-version" class="version"></div>
                <div id="selected-component"></div>
            </div>
        </div>
        <div id="component-root-container">
            <div id="component-listing-container">
                <div id="component-listing" class="component-listing">
                    <div class="section">
                        <div class="header">General</div>
                        <div id="general-links" class="component-links">
                            <ul>
                                <li class="component-item"><a class="document-link overview" href="html/overview.html" target="component-usage">Overview</a></li>
                                <li class="component-item"><a class="document-link getting-started" href="html/getting-started.html" target="component-usage">Getting Started</a></li>
                                <li class="component-item"><a class="document-link user-guide" href="html/user-guide.html" target="component-usage">User Guide</a></li>
                                <li class="component-item"><a class="document-link expression-language-guide" href="html/expression-language-guide.html" target="component-usage">Expression Language Guide</a></li>
                                <li class="component-item"><a class="document-link record-path-guide" href="html/record-path-guide.html" target="component-usage">RecordPath Guide</a></li>
                                <li class="component-item"><a class="document-link admin-guide" href="html/administration-guide.html" target="component-usage">Admin Guide</a></li>
                            </ul>
                            <span class="no-matching no-components hidden">No matching guides</span>
                        </div>
                    </div>
                    <div class="section">
                        <div class="header">Processors</div>
                        <div id="processor-links" class="component-links">
                            <c:choose>
                                <c:when test="${not empty processors}">
                                    <ul>
                                    <c:forEach var="entry" items="${processors}">
                                        <c:forEach var="bundleEntry" items="${processorBundleLookup[entry.value]}">
                                            <li class="component-item">
                                                <span class="bundle-group hidden">${bundleEntry.group}</span>
                                                <span class="bundle-artifact hidden">${bundleEntry.id}</span>
                                                <span class="bundle-version hidden">${bundleEntry.version}</span>
                                                <span class="extension-class hidden">${entry.value}</span>
                                                <a class="component-link" href="components/${bundleEntry.group}/${bundleEntry.id}/${bundleEntry.version}/${entry.value}/index.html" target="component-usage">
                                                    <c:choose>
                                                        <c:when test="${bundleEntry.version == 'unversioned'}">
                                                            ${entry.key}
                                                        </c:when>
                                                        <c:otherwise>
                                                            ${entry.key} <span class="version">${bundleEntry.version}</span>
                                                        </c:otherwise>
                                                    </c:choose>
                                                </a>
                                            </li>
                                        </c:forEach>
                                    </c:forEach>
                                    </ul>
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
                                    <ul>
                                    <c:forEach var="entry" items="${controllerServices}">
                                        <c:forEach var="bundleEntry" items="${controllerServiceBundleLookup[entry.value]}">
                                            <li class="component-item">
                                                <span class="bundle-group hidden">${bundleEntry.group}</span>
                                                <span class="bundle-artifact hidden">${bundleEntry.id}</span>
                                                <span class="bundle-version hidden">${bundleEntry.version}</span>
                                                <span class="extension-class hidden">${entry.value}</span>
                                                <a class="component-link"
                                                   href="components/${bundleEntry.group}/${bundleEntry.id}/${bundleEntry.version}/${entry.value}/index.html" target="component-usage">
                                                    <c:choose>
                                                        <c:when test="${bundleEntry.version == 'unversioned'}">
                                                            ${entry.key}
                                                        </c:when>
                                                        <c:otherwise>
                                                            ${entry.key} <span class="version">${bundleEntry.version}</span>
                                                        </c:otherwise>
                                                    </c:choose>
                                                </a>
                                            </li>
                                        </c:forEach>
                                    </c:forEach>
                                    </ul>
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
                                    <ul>
                                    <c:forEach var="entry" items="${reportingTasks}">
                                        <c:forEach var="bundleEntry" items="${reportingTaskBundleLookup[entry.value]}">
                                            <li class="component-item">
                                                <span class="bundle-group hidden">${bundleEntry.group}</span>
                                                <span class="bundle-artifact hidden">${bundleEntry.id}</span>
                                                <span class="bundle-version hidden">${bundleEntry.version}</span>
                                                <span class="extension-class hidden">${entry.value}</span>
                                                <a class="component-link" href="components/${bundleEntry.group}/${bundleEntry.id}/${bundleEntry.version}/${entry.value}/index.html" target="component-usage">
                                                    <c:choose>
                                                        <c:when test="${bundleEntry.version == 'unversioned'}">
                                                            ${entry.key}
                                                        </c:when>
                                                        <c:otherwise>
                                                            ${entry.key} <span class="version">${bundleEntry.version}</span>
                                                        </c:otherwise>
                                                    </c:choose>
                                                </a>
                                            </li>
                                        </c:forEach>
                                    </c:forEach>
                                    </ul>
                                    <span class="no-matching no-components hidden">No matching reporting tasks</span>
                                </c:when>
                                <c:otherwise>
                                    <span class="no-components">No reporting task documentation found</span>
                                </c:otherwise>
                            </c:choose>
                        </div>
                    </div>
                    <div class="section">
                        <div class="header">Developer</div>
                        <div id="developer-links" class="component-links">
                            <ul>
                                <li class="component-item"><a class="document-link rest-api" href="rest-api/index.html" target="component-usage">Rest Api</a></li>
                                <li class="component-item"><a class="document-link developer-guide" href="html/developer-guide.html" target="component-usage">Developer Guide</a></li>
                                <li class="component-item"><a class="document-link developer-guide" href="html/nifi-in-depth.html" target="component-usage">Apache NiFi In Depth</a></li>
                            </ul>
                            <span class="no-matching no-components hidden">No matching developer guides</span>
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
            </div>
            <div id="component-usage-container">
                <iframe id="component-usage" name="component-usage" frameborder="0" class="component-usage"></iframe>
            </div>
        </div>
        <div id="banner-footer" class="main-banner-footer"></div>
    </body>
</html>
