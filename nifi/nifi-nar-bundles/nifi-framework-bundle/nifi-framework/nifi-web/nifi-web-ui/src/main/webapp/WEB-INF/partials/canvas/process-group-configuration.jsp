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
<div id="process-group-configuration">
    <div class="dialog-content">
        <span id="process-group-id" class="hidden"></span>
        <div class="process-group-setting">
            <div class="setting-name">Process group name</div>
            <div class="setting-field">
                <input type="text" id="process-group-name" name="process-group-name" class="process-group-field"/>
            </div>
        </div>
        <div class="process-group-setting">
            <div class="setting-name">Comments</div>
            <div class="setting-field">
                <textarea cols="30" rows="4" id="process-group-comments" name="process-group-comments" class="process-group-field"></textarea>
            </div>
        </div>
    </div>
</div>