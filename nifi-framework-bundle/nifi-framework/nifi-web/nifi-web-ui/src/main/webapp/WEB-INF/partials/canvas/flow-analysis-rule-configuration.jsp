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
<div id="flow-analysis-rule-configuration" class="hidden large-dialog">
    <div class="flow-analysis-rule-configuration-tab-container dialog-content">
        <div id="flow-analysis-rule-configuration-tabs" class="tab-container"></div>
        <div id="flow-analysis-rule-configuration-tabs-content">
            <div id="flow-analysis-rule-standard-settings-tab-content" class="configuration-tab">
                <div class="settings-left">
                    <div class="setting">
                        <div class="setting-name">Name</div>
                        <div class="flow-analysis-rule-editable setting-field">
                            <input type="text" id="flow-analysis-rule-name" name="flow-analysis-rule-name"/>
                        </div>
                        <div class="flow-analysis-rule-read-only setting-field hidden">
                            <span id="read-only-flow-analysis-rule-name"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">Id</div>
                        <div class="setting-field">
                            <span id="flow-analysis-rule-id"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">Type</div>
                        <div class="setting-field">
                            <span id="flow-analysis-rule-type"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">Bundle</div>
                        <div id="flow-analysis-rule-bundle" class="setting-field"></div>
                    </div>
                </div>
                <div class="spacer">&nbsp;</div>
                <div class="settings-right">
                    <div class="setting">
                        <div class="setting-name">
                            Enforcement Policy
                            <div class="fa fa-question-circle" alt="Info" title="The enforcement policy of the flow analysis rule."></div>
                        </div>
                        <div class="flow-analysis-rule-editable setting-field">
                            <div id="flow-analysis-rule-enforcement-policy-combo"></div>
                        </div>
                        <div class="flow-analysis-rule-read-only setting-field hidden">
                            <span id="read-only-flow-analysis-rule-enforcement-policy"></span>
                        </div>
                    </div>
                </div>
                <div class="clear"></div>
            </div>
            <div id="flow-analysis-rule-properties-tab-content" class="configuration-tab">
                <div id="flow-analysis-rule-properties"></div>
                <div id="flow-analysis-rule-properties-verification-results" class="verification-results">
                    <div class="verification-results-header">Verification Results</div>
                    <div id="flow-analysis-rule-properties-verification-results-listing" class="verification-results-listing"></div>
                </div>
            </div>
            <div id="flow-analysis-rule-comments-tab-content" class="configuration-tab">
                <textarea cols="30" rows="4" id="flow-analysis-rule-comments" name="flow-analysis-rule-comments" class="flow-analysis-rule-editable setting-input"></textarea>
                <div class="setting flow-analysis-rule-read-only hidden">
                    <div class="setting-name">Comments</div>
                    <div class="setting-field">
                        <span id="read-only-flow-analysis-rule-comments"></span>
                    </div>
                </div>
            </div>
        </div>
    </div>
</div>
<div id="new-flow-analysis-rule-property-container"></div>