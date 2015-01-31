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
<div id="processor-property-dialog" class="dialog">
    <div>
        <div class="setting-name">Property name</div>
        <div class="setting-field" id="new-property-name-container">
            <input id="new-property-name" name="new-property-name" type="text"/>
        </div>
        <div class="setting-name" style="margin-top: 36px;">Property value</div>
        <div class="setting-field">
            <div id="new-property-value"></div>
        </div>
    </div>
    <div id="new-property-button-container">
        <div id="new-property-ok" class="button button-normal">Ok</div>
        <div id="new-property-cancel" class="button button-normal">Cancel</div>
        <div class="clear"></div>
    </div>
</div>
