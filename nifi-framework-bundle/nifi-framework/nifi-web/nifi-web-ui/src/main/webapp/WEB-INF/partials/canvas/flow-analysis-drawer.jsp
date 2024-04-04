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
<section id="flow-analysis-drawer">
    <div class="flow-analysis-header">
        <div id="flow-analysis-loading-container" class="flow-analysis-loading-container"></div>
        <div id="flow-analysis-loading-message" class="flow-analysis-loading-message">Rules analysis pending...</div>
    </div>
    <div class="flow-analysis-flow-guide-container">
        <div class="flow-analysis-flow-guide">
            <div class="flow-analysis-flow-guide-title">Flow Guide</div>
            <div>
                <div class="flow-analysis-violations-options">
                    <div class="nf-checkbox checkbox-unchecked" id="show-only-violations"></div>
                    <span class="nf-checkbox-label show-only-violations-label">Show enforced violations</span>
                </div>
                <div class="flow-analysis-warnings-options">
                    <div class="nf-checkbox checkbox-unchecked" id="show-only-warnings"></div>
                    <span class="nf-checkbox-label show-only-warnings-label">Show warning violations</span>
                </div>
            </div>
        </div>
        <div class="flow-analysis-flow-guide-breadcrumb">NiFi Flow</div>
    </div>
    <div id="flow-analysis-rules-accordion" class="flow-analysis-rules-accordion">

        <div id="required-rules" class="required-rules">
            <div>
                <div>Enforced Rules <span id="required-rule-count" class="required-rule-count"></span></div>
            </div>
            <ul id="required-rules-list" class="required-rules-list">
            </ul>
        </div>

        <div id="recommended-rules" class="recommended-rules">
            <div>
                <div>Warning Rules <span id="recommended-rule-count" class="recommended-rule-count"></span></div>
            </div>
            <ul id="recommended-rules-list" class="recommended-rules-list"></ul>
        </div>

        <div id="rule-violations" class="rule-violations">
            <div class="rules-violations-header">
                <div>Enforced Violations <span id="rule-violation-count" class="rule-violation-count"></span></div>
            </div>
            <ul id="rule-violations-list" class="rule-violations-list"></ul>
        </div>

        <div id="rule-warnings" class="rule-warnings">
            <div class="rules-warnings-header">
                <div>Warning Violations <span id="rule-warning-count" class="rule-warning-count"></span></div>
            </div>
            <ul id="rule-warnings-list" class="rule-warnings-list"></ul>
        </div>

        <div class="rule-menu" id="rule-menu">
            <ul>
                <li class="rule-menu-option" id="rule-menu-view-documentation"><i class="fa fa-info-circle rule-menu-option-icon" aria-hidden="true"></i>View Documentation</li>
                <li class="rule-menu-option" id="rule-menu-edit-rule"><i class="fa fa-pencil rule-menu-option-icon" aria-hidden="true"></i>Edit Rule</li>
            </ul>
        </div>

        <div class="violation-menu" id="violation-menu">
            <ul>
                <li class="violation-menu-option" id="violation-menu-more-info"><i class="fa fa-info-circle violation-menu-option-icon" aria-hidden="true"></i>Violation details</li>
                <li class="violation-menu-option" id="violation-menu-go-to"><i class="fa fa-pencil violation-menu-option-icon" aria-hidden="true"></i>Go to component</li>
            </ul>
        </div>
    </div>
</section>