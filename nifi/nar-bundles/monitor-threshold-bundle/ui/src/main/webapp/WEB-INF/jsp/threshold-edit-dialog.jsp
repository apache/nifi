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
<div id="threshold-edit-dialog">
    <div class="dialog-content" >
        <div id="threshold-add-error-message" class="error-message" ></div>

        <input type="hidden" id="attribute-row-id"></input>

        <div class="setting">
            <div class="setting-name">Attribute Value</div>
            <div class="setting-field">
                <input id="edit-attribute-value" name="new-property-value" readonly="readonly" style="width: 200px;" />
            </div>
        </div>
        <div class="setting">
            <div class="setting-name">Size Threshold (bytes)</div>
            <div class="setting-field">
                <input id="edit-size-value" name="new-property-value" style="width: 200px;" type="text"/>
            </div>
        </div>
        <div class="setting">
            <div class="setting-name">File Threshold</div>
            <div class="setting-field">
                <input id="edit-file-count-value" name="new-property-value" style="width: 200px;" type="text"/>
            </div>
        </div>

    </div>

</div>

