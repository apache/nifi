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
<div id="parameter-provider-configuration" class="hidden large-dialog">
    <div class="parameter-provider-configuration-tab-container dialog-content">
        <div id="parameter-provider-configuration-tabs" class="tab-container"></div>
        <div id="parameter-provider-configuration-tabs-content">
            <div id="parameter-provider-standard-settings-tab-content" class="configuration-tab">
                <div class="settings-left">
                    <div class="setting">
                        <div class="setting-name">Name</div>
                        <div class="parameter-provider-editable setting-field">
                            <input type="text" id="parameter-provider-name" name="parameter-provider-name"/>
                        </div>
                        <div class="parameter-provider-read-only setting-field hidden">
                            <span id="read-only-parameter-provider-name"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">Id</div>
                        <div class="setting-field">
                            <span id="parameter-provider-id"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">Type</div>
                        <div class="setting-field">
                            <span id="parameter-provider-type"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">Bundle</div>
                        <div id="parameter-provider-bundle" class="setting-field"></div>
                    </div>
                </div>
                <div class="spacer">&nbsp;</div>
                <div class="settings-right">
                    <div class="setting">
                        <div class="setting-name">
                            Referencing Components
                            <div class="fa fa-question-circle" alt="Info" title="Other components referencing this parameter provider."></div>
                        </div>
                        <div class="setting-field">
                            <div id="parameter-provider-referencing-components"></div>
                        </div>
                    </div>
                </div>
            </div>
            <div id="parameter-provider-properties-tab-content" class="configuration-tab">
                <div id="parameter-provider-properties"></div>
                <div id="parameter-provider-properties-verification-results" class="verification-results">
                    <div class="verification-results-header">Verification Results</div>
                    <div id="parameter-provider-properties-verification-results-listing" class="verification-results-listing"></div>
                </div>
            </div>
            <div id="parameter-provider-comments-tab-content" class="configuration-tab">
                <textarea cols="30" rows="4" id="parameter-provider-comments" name="parameter-provider-comments" class="parameter-provider-editable setting-input"></textarea>
                <div class="setting parameter-provider-read-only hidden">
                    <div class="setting-name">Comments</div>
                    <div class="setting-field">
                        <span id="read-only-parameter-provider-comments"></span>
                    </div>
                </div>
            </div>
        </div>
    </div>
</div>
<div id="new-parameter-provider-property-container"></div>
