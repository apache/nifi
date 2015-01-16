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
<div id="templates">
    <div id="templates-header-and-filter">
        <div id="templates-header-text">NiFi Templates</div>
        <div id="templates-filter-controls">
            <div id="templates-filter-container">
                <input type="text" id="templates-filter"/>
                <div id="templates-filter-type"></div>
            </div>
            <div id="templates-filter-stats">
                Displaying&nbsp;<span id="displayed-templates"></span>&nbsp;of&nbsp;<span id="total-templates"></span>
            </div>
        </div>
    </div>
    <div id="templates-refresh-container">
        <div id="refresh-button" class="templates-refresh pointer" title="Refresh"></div>
        <div id="templates-last-refreshed-container">
            Last updated:&nbsp;<span id="templates-last-refreshed"></span>
        </div>
        <div id="templates-loading-container" class="loading-container"></div>
        <div id="upload-template-container" class="hidden">
            <div id="select-template-container">
                <div id="template-browse-container">
                    <div id="select-template-button" class="template-button">
                        <span>Browse</span>
                        <form id="template-upload-form" enctype="multipart/form-data" method="post" action="../nifi-api/controller/templates">
                            <input type="file" name="template" id="template-file-field"/>
                        </form>
                    </div>
                    <div id="upload-template-status" class="import-status"></div>
                </div>
            </div>
            <div id="submit-template-container">
                <div id="upload-template-button" class="template-button">Import</div>
                <div id="cancel-upload-template-button" class="template-button">Cancel</div>
                <div id="selected-template-name"></div>
            </div>
            <div id="template-upload-form-container">
            </div>
        </div>
    </div>
    <div id="templates-table"></div>
</div>