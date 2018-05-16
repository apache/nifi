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
<div id="reporting-task-configuration" class="hidden large-dialog">
    <div class="reporting-task-configuration-tab-container dialog-content">
        <div id="reporting-task-configuration-tabs" class="tab-container"></div>
        <div id="reporting-task-configuration-tabs-content">
            <div id="reporting-task-standard-settings-tab-content" class="configuration-tab">
                <div class="settings-left">
                    <div class="setting">
                        <div class="setting-name">Name</div>
                        <div class="reporting-task-editable setting-field">
                            <input type="text" id="reporting-task-name" name="reporting-task-name"/>
                            <div class="reporting-task-enabled-container">
                                <div id="reporting-task-enabled" class="nf-checkbox checkbox-unchecked"></div>
                                <span class="nf-checkbox-label"> Enabled</span>
                            </div>
                        </div>
                        <div class="reporting-task-read-only setting-field hidden">
                            <span id="read-only-reporting-task-name"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">Id</div>
                        <div class="setting-field">
                            <span id="reporting-task-id"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">Type</div>
                        <div class="setting-field">
                            <span id="reporting-task-type"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">Bundle</div>
                        <div id="reporting-task-bundle" class="setting-field"></div>
                    </div>
                </div>
                <div class="spacer">&nbsp;</div>
                <div class="settings-right">
                    <div class="setting">
                        <div class="setting-name">
                            Scheduling strategy
                            <div class="fa fa-question-circle" alt="Info" title="The strategy used to schedule this reporting task."></div>
                        </div>
                        <div class="reporting-task-editable setting-field">
                            <div id="reporting-task-scheduling-strategy-combo"></div>
                        </div>
                        <div class="reporting-task-read-only setting-field hidden">
                            <span id="read-only-reporting-task-scheduling-strategy"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">
                            Run schedule
                            <div class="fa fa-question-circle" alt="Info" title="The amount of time that should elapse between task executions."></div>
                        </div>
                        <div class="reporting-task-editable setting-field">
                            <input type="text" id="reporting-task-timer-driven-scheduling-period" class="reporting-task-scheduling-period"/>
                            <input type="text" id="reporting-task-cron-driven-scheduling-period" class="reporting-task-scheduling-period"/>
                        </div>
                        <div class="reporting-task-read-only setting-field hidden">
                            <span id="read-only-reporting-task-scheduling-period"></span>
                        </div>
                    </div>
                </div>
                <div class="clear"></div>
            </div>
            <div id="reporting-task-properties-tab-content" class="configuration-tab">
                <div id="reporting-task-properties"></div>
            </div>
            <div id="reporting-task-comments-tab-content" class="configuration-tab">
                <textarea cols="30" rows="4" id="reporting-task-comments" name="reporting-task-comments" class="reporting-task-editable setting-input"></textarea>
                <div class="setting reporting-task-read-only hidden">
                    <div class="setting-name">Comments</div>
                    <div class="setting-field">
                        <span id="read-only-reporting-task-comments"></span>
                    </div>
                </div>
            </div>
        </div>
    </div>
</div>
<div id="new-reporting-task-property-container"></div>