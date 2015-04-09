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
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
        <link rel="shortcut icon" href="../nifi/images/nifi16.ico"/>
        <title>NiFi</title>
        <link href="css/main.css" rel="stylesheet" type="text/css" />
        <link href="../nifi/css/message-pane.css" rel="stylesheet" type="text/css" />
        <link href="../nifi/css/message-page.css" rel="stylesheet" type="text/css" />
        <link rel="stylesheet" href="../nifi/js/jquery/combo/jquery.combo.css" type="text/css" />
        <link rel="stylesheet" href="../nifi/css/reset.css" type="text/css" />
        <script type="text/javascript" src="../nifi/js/jquery/jquery-2.1.1.min.js"></script>
        <script type="text/javascript" src="../nifi/js/jquery/combo/jquery.combo.js"></script>
        <script type="text/javascript">
            var $$ = $.noConflict(true);
            $$(document).ready(function () {
                var url = $$('#requestUrl').text();
                var ref = $$('#ref').text();
                
                // create the parameters
                var params = {
                    ref: ref
                };
                
                // include the cluster node if appropriate
                var clusterNodeId = $$('#clusterNodeId').text();
                if (clusterNodeId !== '') {
                    params['clusterNodeId'] = clusterNodeId;
                }
                
                // determine the appropriate mode to select initially
                var initialMode = $$('#mode').text();
                if (initialMode === '') {
                    initialMode = 'Original';
                }
                
                var currentLocation = null;
                $$('#view-as').combo({
                    options: [{
                            text: 'original',
                            value: 'Original'
                        }, {
                            text: 'formatted',
                            value: 'Formatted'
                        }, {
                            text: 'hex',
                            value: 'Hex'
                        }],
                    selectedOption: {
                        value: initialMode
                    },
                    select: function (option) {
                        // just record the selection during creation
                        if (currentLocation === null) {
                            currentLocation = option.value;
                            return;
                        }
                        
                        // if the selection has changesd, reload the page
                        if (currentLocation !== option.value) {
                            window.location.href = url + '?' + $$.param($$.extend({
                                mode: option.value
                            }, params));
                        }
                    }
                });
            });
        </script>
    </head>
    <body class="message-pane">
        <span id="ref" class="hidden"><%= org.apache.nifi.util.EscapeUtils.escapeHtml(request.getParameter("ref")) %></span>
        <span id="clusterNodeId" class="hidden"><%= request.getParameter("clusterNodeId") == null ? "" : org.apache.nifi.util.EscapeUtils.escapeHtml(request.getParameter("clusterNodeId")) %></span>
        <span id="mode" class="hidden"><%= request.getParameter("mode") == null ? "" : org.apache.nifi.util.EscapeUtils.escapeHtml(request.getParameter("mode")) %></span>
        <span id="requestUrl" class="hidden"><%= org.apache.nifi.util.EscapeUtils.escapeHtml(request.getRequestURL().toString()) %></span>
        <div id="view-as-label">View as</div>
        <div id="view-as" class="pointer button-normal"></div>
        <div id="content-filename"><span class="content-label">filename</span><%= request.getAttribute("filename") == null ? "" : org.apache.nifi.util.EscapeUtils.escapeHtml(request.getAttribute("filename").toString()) %></div>
        <div id="content-type"><span class="content-label">content type</span><%= request.getAttribute("contentType") == null ? "" : org.apache.nifi.util.EscapeUtils.escapeHtml(request.getAttribute("contentType").toString()) %></div>
        <div class="message-pane-message-box">