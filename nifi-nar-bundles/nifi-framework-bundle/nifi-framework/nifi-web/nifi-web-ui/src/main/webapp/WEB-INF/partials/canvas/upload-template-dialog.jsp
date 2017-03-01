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
<div id="upload-template-dialog" class="hidden small-dialog">
    <div class="dialog-content">
        <div id="select-template-container">
            <div id="template-browse-container">
                <span id="select-template-label">Select Template</span>
                <div id="select-template-button">
                    <button class="fa fa-search" id="template-file-field-button" title="Browse"></button>
                    <form id="template-upload-form" enctype="multipart/form-data" method="post">
                        <input type="file" name="template" id="template-file-field"/>
                    </form>
                </div>
            </div>
        </div>
        <div id="submit-template-container">
            <div id="selected-template-name"></div>
        </div>
        <div id="upload-template-status" class="import-status"></div>
    </div>
</div>