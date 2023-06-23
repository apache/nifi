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
    <div id="process-group-configuration-header-text" class="settings-header-text">Process Group Configuration</div>
    <div class="settings-container">
        <div>
            <div id="process-group-configuration-tabs" class="settings-tabs tab-container"></div>
            <div class="clear"></div>
        </div>
        <div id="process-group-configuration-tabs-content">
            <button id="add-process-group-configuration-controller-service" class="add-button fa fa-plus" title="Create a new controller service"></button>
            <div id="general-process-group-configuration-tab-content" class="configuration-tab">
                <div id="general-process-group-configuration">
                    <div class="setting">
                        <div class="setting-name">Process group name</div>
                        <span id="process-group-id" class="hidden"></span>
                        <div class="editable setting-field">
                            <input type="text" id="process-group-name" class="setting-input"/>
                        </div>
                        <div class="read-only setting-field">
                            <span id="read-only-process-group-name" class="unset"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">Process group parameter context</div>
                        <div class="editable setting-field">
                            <div id="process-group-parameter-context-combo"></div>
                            <div id="parameter-contexts-recursive-container">
                                <div id="parameter-contexts-recursive" class="nf-checkbox checkbox-unchecked"></div>
                                <div class="nf-checkbox-label">Apply recursively</div>
                                <div class="fa fa-question-circle" alt="Info" title="When checked Parameter Context will be applied to the Process Group and all the embedded Process Groups recursively, if the user has the proper permissions on all the respective components. If the user does not have the proper permissions on any embedded Process Group, then the Parameter Context will not be applied for any components."></div>
                            </div>
                            <div class="clear"></div>
                        </div>
                        <div class="read-only setting-field">
                            <span id="read-only-process-group-parameter-context" class="unset">Unauthorized</span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">Process group comments</div>
                        <div class="editable setting-field">
                            <textarea id="process-group-comments" class="setting-input"></textarea>
                        </div>
                        <div class="read-only setting-field">
                            <span id="read-only-process-group-comments" class="unset"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">Process group FlowFile concurrency</div>
                        <div class="editable setting-field">
                            <div id="process-group-flowfile-concurrency-combo"></div>
                        </div>
                        <div class="read-only setting-field">
                            <span id="read-only-process-group-flowfile-concurrency" class="unset"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">Process group outbound policy</div>
                        <div class="editable setting-field">
                            <div id="process-group-outbound-policy-combo"></div>
                        </div>
                        <div class="read-only setting-field">
                            <span id="read-only-process-group-outbound-policy" class="unset"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">Default FlowFile Expiration</div>
                        <div class="editable setting-field">
                            <input type="text" id="process-group-default-flowfile-expiration" class="setting-input"/>
                        </div>
                        <div class="read-only setting-field">
                            <span id="read-only-process-group-default-flowfile-expiration" class="unset"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">Default Back Pressure Object Threshold</div>
                        <div class="editable setting-field">
                            <input type="text" id="process-group-default-back-pressure-object-threshold" class="setting-input"/>
                        </div>
                        <div class="read-only setting-field">
                            <span id="read-only-process-group-default-back-pressure-object-threshold" class="unset"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">Default Back Pressure Data Size Threshold</div>
                        <div class="editable setting-field">
                            <input type="text" id="process-group-default-back-pressure-data-size-threshold" class="setting-input"/>
                        </div>
                        <div class="read-only setting-field">
                            <span id="read-only-process-group-default-back-pressure-data-size-threshold" class="unset"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">Log File Suffix
                        <div class="fa fa-question-circle" alt="Info" title="Turns on dedicated logging. When left empty log messages will be logged only to the primary app log. When set messages logged by components in this group will be sent to the standard app log, as well as a separate log file, with the provided name, specific to this group."></div>
                        </div>
                        <div class="editable setting-field">
                            <input type="text" id="process-group-log-file-suffix" class="setting-input"/>
                        </div>
                        <div class="read-only setting-field">
                            <span id="read-only-process-group-log-file-suffix" class="unset"></span>
                        </div>
                    </div>

                    <div class="editable settings-buttons">
                        <div id="process-group-configuration-save" class="button">Apply</div>
                        <div class="clear"></div>
                    </div>
                </div>
            </div>
            <div id="process-group-controller-services-tab-content" class="configuration-tab">
                <div id="process-group-controller-services-table" class="settings-table"></div>
            </div>
        </div>
    </div>
    <div id="process-group-refresh-container">
        <button id="process-group-configuration-refresh-button" class="refresh-button pointer fa fa-refresh" title="Refresh"></button>
        <div class="last-refreshed-container">
            Last updated:&nbsp;<span id="process-group-configuration-last-refreshed" class="last-refreshed"></span>
        </div>
        <div id="process-group-configuration-loading-container" class="loading-container"></div>
        <div id="flow-cs-availability" class="hidden">Listed services are available to all descendant Processors and services of this Process Group.</div>
        <div class="clear"></div>
    </div>
</div>
