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
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <link rel="stylesheet" href="../nifi/js/jquery/ui-smoothness/jquery-ui-1.10.4.min.css" type="text/css" />
        <link rel="stylesheet" href="../nifi/js/jquery/slickgrid/css/slick.grid.css" type="text/css" />
        <link rel="stylesheet" href="../nifi/js/jquery/slickgrid/css/slick-default-theme.css" type="text/css" />
        <link rel="stylesheet" href="../nifi/js/jquery/modal/jquery.modal.css" type="text/css" />
        <link rel="stylesheet" href="../nifi/js/jquery/combo/jquery.combo.css" type="text/css" />
        <link rel="stylesheet" href="../nifi/js/jquery/qtip2/jquery.qtip.min.css" type="text/css" />
        <link rel="stylesheet" href="../nifi/js/codemirror/lib/codemirror.css" type="text/css" />
        <link rel="stylesheet" href="../nifi/js/codemirror/addon/hint/show-hint.css" type="text/css" />
        <link rel="stylesheet" href="../nifi/js/jquery/nfeditor/jquery.nfeditor.css" type="text/css" />
        <link rel="stylesheet" href="../nifi/js/jquery/nfeditor/languages/nfel.css" type="text/css" />
        <link rel="stylesheet" href="../nifi/css/reset.css" type="text/css" />
        <link rel="stylesheet" href="css/main.css" type="text/css" />
        <script type="text/javascript" src="../nifi/js/jquery/jquery-2.1.1.min.js"></script>
        <script type="text/javascript" src="../nifi/js/jquery/jquery.center.js"></script>
        <script type="text/javascript" src="../nifi/js/jquery/jquery.each.js"></script>
        <script type="text/javascript" src="../nifi/js/jquery/jquery.tab.js"></script>
        <script type="text/javascript" src="../nifi/js/jquery/modal/jquery.modal.js"></script>
        <script type="text/javascript" src="../nifi/js/jquery/combo/jquery.combo.js"></script>
        <script type="text/javascript" src="../nifi/js/jquery/jquery.ellipsis.js"></script>
        <script type="text/javascript" src="../nifi/js/jquery/ui-smoothness/jquery-ui-1.10.4.min.js"></script>
        <script type="text/javascript" src="../nifi/js/jquery/qtip2/jquery.qtip.min.js"></script>
        <script type="text/javascript" src="../nifi/js/json2.js"></script>
        <script type="text/javascript" src="../nifi/js/jquery/jquery.event.drag-2.2.min.js"></script>
        <script type="text/javascript" src="../nifi/js/jquery/slickgrid/plugins/slick.cellrangedecorator.js"></script>
        <script type="text/javascript" src="../nifi/js/jquery/slickgrid/plugins/slick.cellrangeselector.js"></script>
        <script type="text/javascript" src="../nifi/js/jquery/slickgrid/plugins/slick.cellselectionmodel.js"></script>
        <script type="text/javascript" src="../nifi/js/jquery/slickgrid/plugins/slick.rowselectionmodel.js"></script>
        <script type="text/javascript" src="../nifi/js/jquery/slickgrid/slick.formatters.js"></script>
        <script type="text/javascript" src="../nifi/js/jquery/slickgrid/slick.editors.js"></script>
        <script type="text/javascript" src="../nifi/js/jquery/slickgrid/slick.dataview.js"></script>
        <script type="text/javascript" src="../nifi/js/jquery/slickgrid/slick.core.js"></script>
        <script type="text/javascript" src="../nifi/js/jquery/slickgrid/slick.grid.js"></script>
        <script type="text/javascript" src="../nifi/js/codemirror/lib/codemirror-compressed.js"></script>
        <script type="text/javascript" src="../nifi/js/nf/nf-namespace.js"></script>
        <script type="text/javascript" src="../nifi/js/jquery/nfeditor/languages/nfel.js"></script>
        <script type="text/javascript" src="../nifi/js/jquery/nfeditor/jquery.nfeditor.js"></script>
        <script type="text/javascript" src="js/application.js"></script>
        <title>Update Attribute</title>
    </head>
    <body>
        <div id="attribute-updater-processor-id" class="hidden"><%= request.getParameter("id") == null ? "" : org.apache.nifi.util.EscapeUtils.escapeHtml(request.getParameter("id")) %></div>
        <div id="attribute-updater-client-id" class="hidden"><%= request.getParameter("clientId") == null ? "" : org.apache.nifi.util.EscapeUtils.escapeHtml(request.getParameter("clientId")) %></div>
        <div id="attribute-updater-revision" class="hidden"><%= request.getParameter("revision") == null ? "" : org.apache.nifi.util.EscapeUtils.escapeHtml(request.getParameter("revision")) %></div>
        <div id="attribute-updater-editable" class="hidden"><%= request.getParameter("editable") == null ? "" : org.apache.nifi.util.EscapeUtils.escapeHtml(request.getParameter("editable")) %></div>
        <div id="update-attributes-content">
            <div id="rule-list-panel">
                <div id="flowfile-policy-container">
                    <span id="selected-flowfile-policy" class="hidden"></span>
                    <div id="flowfile-policy-label" class="large-label">FlowFile Policy</div>
                    <img class="icon-info" src="images/iconInfo.png" alt="Info" title="Defines the behavior when multiple rules match. Use clone will ensure that each matching rule is executed with a copy of the original flowfile. Use original will execute all matching rules with the original flowfile in the order specified below."/>
                    <div id="flowfile-policy"></div>
                    <div class="clear"></div>
                </div>
                <div id="rule-label-container">
                    <div id="rules-label" class="large-label">Rules</div>
                    <img class="icon-info" src="images/iconInfo.png" alt="Info" title="Click and drag to change the order that rules are evaluated."/>
                    <div id="new-rule" class="new-rule hidden"></div>
                    <div class="clear"></div>
                </div>
                <div id="rule-list-container">
                    <ul id="rule-list"></ul>
                </div>
                <div id="no-rules" class="unset">No rules found.</div>
                <div id="rule-filter-controls" class="hidden">
                    <div id="rule-filter-container">
                        <input type="text" id="rule-filter"/>
                        <div id="rule-filter-type"></div>
                    </div>
                    <div id="rule-filter-stats">
                        Displaying&nbsp;<span id="displayed-rules"></span>&nbsp;of&nbsp;<span id="total-rules"></span>
                    </div>
                </div>
            </div>
            <div id="rule-details-panel">
                <div id="selected-rule-name-container" class="selected-rule-detail">
                    <div class="large-label">Rule Name</div>
                    <div id="selected-rule-id" class="hidden"></div>
                    <div id="no-rule-selected-label" class="unset">No rule selected.</div>
                    <input type="text" id="selected-rule-name" class="hidden"></input>
                </div>
                <div id="selected-rule-conditions-container" class="selected-rule-detail">
                    <div class="large-label-container">
                        <div id="conditions-label" class="large-label">Conditions</div>
                        <img class="icon-info" src="images/iconInfo.png" alt="Info" title="All conditions must be met for this rule to match."/>
                        <div id="new-condition" title="New Condition" class="new-condition hidden"></div>
                        <div class="clear"></div>
                    </div>
                    <div id="selected-rule-conditions"></div>
                </div>
                <div id="selected-rule-actions-container" class="selected-rule-detail">
                    <div class="large-label-container">
                        <div id="actions-label" class="large-label">Actions</div>
                        <div id="new-action" title="New Action" class="new-action hidden"></div>
                        <div class="clear"></div>
                    </div>
                    <div id="selected-rule-actions"></div>
                </div>
                <div style="margin-top: -10px;">
                    <div id="message"></div>
                    <div id="selected-rule-save" class="button hidden">Save</div>
                    <div class="clear"></div>
                </div>
                <div class="clear"></div>
            </div>
            <div class="clear"></div>
            <div id="glass-pane"></div>
            <div id="ok-dialog">
                <div id="ok-dialog-content" class="dialog-content"></div>
            </div>
            <div id="yes-no-dialog">
                <div id="yes-no-dialog-content" class="dialog-content"></div>
            </div>
            <div id="new-rule-dialog">
                <div class="dialog-content">
                    <div class="rule-setting">
                        <div class="label">Rule name</div>
                        <div>
                            <input id="new-rule-name" type="text" />
                        </div>
                    </div>
                    <div class="rule-setting">
                        <div class="label">Copy from existing rule (optional)</div>
                        <div>
                            <input id="copy-from-rule-name" type="text" class="search" />
                        </div>
                    </div>
                </div>
            </div>
            <div id="new-condition-dialog" class="dialog">
                <div style="margin-bottom: 32px;">
                    <div class="rule-setting">
                        <div class="label">Expression</div>
                        <div>
                            <div id="new-condition-expression"></div>
                        </div>
                    </div>
                </div>
                <div id="new-condition-button-container">
                    <div id="new-condition-add" class="button button-normal">Add</div>
                    <div id="new-condition-cancel" class="button button-normal">Cancel</div>
                    <div class="clear"></div>
                </div>
            </div>
            <div id="new-action-dialog" class="dialog">
                <div style="margin-bottom: 32px;">
                    <div class="rule-setting">
                        <div class="label">Attribute</div>
                        <div id="new-action-attribute-container">
                            <input id="new-action-attribute" type="text"></input>
                        </div>
                    </div>
                    <div class="rule-setting" style="margin-top: 36px;">
                        <div class="label">Value</div>
                        <div>
                            <div id="new-action-value"></div>
                        </div>
                    </div>
                </div>
                <div id="new-action-button-container">
                    <div id="new-action-add" class="button button-normal">Add</div>
                    <div id="new-action-cancel" class="button button-normal">Cancel</div>
                    <div class="clear"></div>
                </div>
            </div>
        </div>
    </body>
</html>