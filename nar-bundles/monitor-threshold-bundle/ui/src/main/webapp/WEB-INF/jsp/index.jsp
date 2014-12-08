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
<%@page contentType="text/html" pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
    <head>
        <title>Threshold Settings</title>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
        <meta http-equiv="X-UA-Compatible" content="IE=EmulateIE7"/>
        <link rel="shortcut icon" href="images/nifi16.ico"/>

        <link rel="stylesheet" href="../nifi/js/jquery/css/smoothness/jquery-ui-1.8.10.custom.css" type="text/css" />
        <link rel="stylesheet" href="../nifi/js/jquery/modal/jquery.modal.css" type="text/css" />

        <link rel="stylesheet" href="js/jquery/jqgrid/css/ui.jqgrid.css" type="text/css" />

        <!-- Following is missing.  Not sure where it might be.  Will need to search further.  -->
        <link rel="stylesheet" href="../nifi/css/nf-canvas-all.css" type="text/css" />

        <link rel="stylesheet" href="css/threshold_styles.css" type="text/css" />

        <script type="text/javascript" src="../nifi/js/jquery/jquery-1.7.min.js"></script>
        <script type="text/javascript" src="../nifi/js/jquery/jquery-ui-1.8.10.custom.min.js"></script>
        <script type="text/javascript" src="../nifi/js/jquery/jquery.center.js"></script>
        <script type="text/javascript" src="../nifi/js/jquery/modal/jquery.modal.js"></script>

        <script type="text/javascript" src="js/jquery/jqgrid/js/i18n/grid.locale-en.js"></script>
        <script type="text/javascript" src="js/jquery/jqgrid/js/jquery.jqGrid.min.js"></script>
        <script type="text/javascript" src="js/nf-common.js" ></script>

    </head>
    <body>
        <jsp:include page="/WEB-INF/jsp/error-dialog.jsp"/>
        <jsp:include page="/WEB-INF/jsp/attribute-confirm-dialog.jsp"/>
        <jsp:include page="/WEB-INF/jsp/attribute-add-dialog.jsp"/>
        <jsp:include page="/WEB-INF/jsp/threshold-add-dialog.jsp"/>
        <jsp:include page="/WEB-INF/jsp/threshold-confirm-dialog.jsp"/>
        <jsp:include page="/WEB-INF/jsp/attribute-edit-dialog.jsp"/>
        <jsp:include page="/WEB-INF/jsp/threshold-edit-dialog.jsp"/>
        <jsp:include page="/WEB-INF/jsp/attribute-filter-dialog.jsp"/>
        <jsp:include page="/WEB-INF/jsp/threshold-filter-dialog.jsp"/>

        <div id="threshold-header-text">Threshold Settings </div>

        <div  class="list-header" >
            <div id="control-box"> 
                <div id="save-cancel">
                    <div id="attribute-list-header" class="list-header-text">Attributes</div>
                    <img alt="Separator" src="images/separator.gif">
                    <input id="attribute-add-main"  class="control-style" type="image" src="images/addWorksheetRow.png" title = "Add New Attribute" />
                    <div id="attribute-filter-values" class="clear-filter"></div>
                </div>
            </div>
            <div id="filter-selection" style="left:20px;">
                <table width="80%">
                    <tr>
                        <td><div id="attribute-names-header" class="abbreviated-header" >Names</div></td>
                    </tr>
                </table>
            </div>
        </div>
        <div id="list1" class="list_container">
            <table id="list"></table>

        </div>
        <div id="rowdetails"></div>
        <div id="status-bar" style="position: relative; bottom: 0px; padding: 2px; height: 15px; width:100%; display: inline-block; background-color: silver; color: black;" ></div>

        <div id="faded-background"></div>
        <div id="glass-pane"></div>

        <div id="attribute-context-menu" class="context-menu">
            <div id="attribute-add" class="context-control" ><img src="images/addWorksheetRow.png"/> <span class="context-label">Add Attribute</span></div>
            <div id="attribute-edit" class="context-control" ><img style="width:16px; height:16px;" src="../../nifi/images/iconEdit.png"/> <span class="context-label">Edit Attribute</span></div>            
            <div id="attribute-delete" class="context-control" ><img src="images/removeWorksheetRow.png"/> <span class="context-label">Delete Attribute</span></div>
            <div id="attribute-filter" class="context-control" ><img src="images/filter.gif" /> <span class="context-label">Filter Attributes</span></div>
        </div>

        <div id="threshold-context-menu" class="context-menu">
            <div id="threshold-add" class="context-control" ><img src="images/addWorksheetRow.png"/> <span class="context-label">Add Threshold</span></div>
            <div id="threshold-edit" class="context-control" ><img style="width:16px; height:16px;" src="../../nifi/images/iconEdit.png"/> <span class="context-label">Edit Threshold</span></div>            
            <div id="threshold-delete" class="context-control" ><img src="images/removeWorksheetRow.png"/> <span class="context-label">Delete Threshold</span></div>
            <div id="threshold-filter" class="context-control" ><img src="images/filter.gif" /> <span class="context-label">Filter Thresholds</span></div>
        </div>
    </body>
</html>

