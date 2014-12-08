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
<div id="threshold-dialog">
    <div class="dialog-content" >
        <div id="attribute-info" ></div>   
        <input type="hidden" id="attribute-row-id"></input>
        <input type="hidden" id="attribute-row-name"></input>
        <div id="threshold-list-header" class="dialog-list-header" >
            <div id="control-box"> 
                <div id="save-cancel">
                    <input id="threshold-add"  class="control-style" type="image" src="images/addWorksheetRow.png" title = "Add New Threshold" />
                    <input id="threshold-save"  class="control-style" type="image" src="../nifi-web/images/iconCommit.png" title = "Save Threshold" />
                    <input id="threshold-cancel"  class="control-style" type="image" src="../nifi-web/images/iconUndo.png" title = "Cancel Edits" />
                    <input id="threshold-delete"  class="control-style" type="image" src="images/removeWorksheetRow.png" title = "Delete Selected Thresholds" />
                    <img alt="Separator" src="images/separator.gif">
                    <input id="threshold-filter"  class="control-style" type="image" src="images/filter.gif" title = "Filter Attributes" />
                    <input id="threshold-clear-filter"  class="control-style" type="image" src="images/clear.png" title = "Clear Threshold Filters" />
                </div>
            </div>
            <div id="filter-selection">
                <table width="95%">
                    <tr>
                        <td width="365px"><input id="attributevaluefilter"  class="filter-text" type="text" title = "Enter Atribute Value Filter Criteria" ></input></td>
                        <td width="210px"><input id="sizefilter" style="width:80px;"  class="filter-text" type="text" title = "Enter Size Filter Criteria" ></input></td>
                        <td ><input id="filecountfilter" style="width:80px;" class="filter-text" type="text" title = "Enter File Count Filter Criteria" ></input></td>
                    </tr>
                </table>
            </div>
        </div>
        <div id="threshold-list-container"></div>
    </div> 

</div>

