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
<div id="processor-details">
    <div class="processor-details-tab-container">
        <div id="processor-details-tabs"></div>
        <div id="processor-details-tabs-content">
            <div id="details-standard-settings-tab-content" class="details-tab">
                <div class="settings-left">
                    <div class="setting">
                        <div class="setting-name">Name</div>
                        <div class="setting-field">
                            <span id="read-only-processor-name"></span>
                        </div>
                        <div class="clear"></div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">Id</div>
                        <div class="setting-field">
                            <span id="read-only-processor-id"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">Type</div>
                        <div class="setting-field">
                            <span id="read-only-processor-type"></span>
                        </div>
                        <div class="clear"></div>
                    </div>
                    <div class="setting">
                        <div class="penalty-duration-setting">
                            <div class="setting-name">
                                Penalty duration
                                <img class="setting-icon icon-info" src="images/iconInfo.png" alt="Info" title="The amount of time used when this processor penalizes a FlowFile."/>
                            </div>
                            <div class="setting-field">
                                <span id="read-only-penalty-duration"></span>
                            </div>
                        </div>
                        <div class="yield-duration-setting">
                            <div class="setting-name">
                                Yield duration
                                <img class="setting-icon icon-info" src="images/iconInfo.png" alt="Info" title="When a processor yields, it will not be scheduled again until this amount of time elapses."/>
                            </div>
                            <div class="setting-field">
                                <span id="read-only-yield-duration"></span>
                            </div>
                        </div>
                        <div class="clear"></div>
                    </div>
                    <div class="setting">
                        <div class="bulletin-setting">
                            <div class="setting-name">
                                Bulletin level
                                <img class="setting-icon icon-info" src="images/iconInfo.png" alt="Info" title="The level at which this processor will generate bulletins."/>
                            </div>
                            <div class="setting-field">
                                <span id="read-only-bulletin-level"></span>
                            </div>
                        </div>
                        <div class="clear"></div>
                    </div>
                </div>
                <div class="spacer">&nbsp;</div>
                <div class="settings-right">
                    <div class="setting">
                        <div class="setting-name">
                            Auto terminate relationships
                            <img class="setting-icon icon-info" src="images/iconInfo.png" alt="Info" title="Will automatically terminate FlowFiles sent to all relationships in bold."/>
                        </div>
                        <div class="setting-field">
                            <div id="read-only-auto-terminate-relationship-names"></div>
                        </div>
                    </div>
                </div>
            </div>
            <div id="details-scheduling-tab-content" class="details-tab">
                <div class="settings-left">
                    <div class="setting">
                        <div class="scheduling-strategy-setting">
                            <div class="setting-name">
                                Scheduling strategy
                                <img class="setting-icon icon-info" src="images/iconInfo.png" alt="Info" title="The strategy used to schedule this processor."/>
                            </div>
                            <div class="setting-field">
                                <span id="read-only-scheduling-strategy"></span>
                            </div>
                        </div>
                        <div class="clear"></div>
                    </div>
                    <div class="setting">
                        <div class="concurrently-schedulable-tasks-setting">
                            <div class="setting-name">
                                Concurrent tasks
                                <img class="setting-icon icon-info" src="images/iconInfo.png" alt="Info" title="The number of tasks that should be concurrently scheduled for this processor."/>
                            </div>
                            <div class="setting-field">
                                <span id="read-only-concurrently-schedulable-tasks"></span>
                            </div>
                        </div>
                        <div id="read-only-run-schedule" class="scheduling-period-setting">
                            <div class="setting-name">
                                Run schedule
                                <img class="setting-icon icon-info" src="images/iconInfo.png" alt="Info" title="The minimum number of seconds that should elapse between task executions."/>
                            </div>
                            <div class="setting-field">
                                <span id="read-only-scheduling-period"></span>
                            </div>
                        </div>
                        <div class="clear"></div>
                    </div>
                </div>
                <div class="spacer">&nbsp;</div>
                <div class="settings-right">
                    <div class="setting">
                        <div class="setting-name">
                            Run duration
                            <img class="setting-icon icon-info" src="images/iconInfo.png" alt="Info" title="The amount of time this processor will run for when scheduled."/>
                        </div>
                        <div class="setting-field">
                            <span id="read-only-run-duration"></span>
                        </div>
                    </div>
                </div>
            </div>
            <div id="details-processor-properties-tab-content" class="details-tab">
                <div id="read-only-processor-properties"></div>
            </div>
            <div id="details-processor-comments-tab-content" class="details-tab">
                <div class="setting">
                    <div class="setting-name">Comments</div>
                    <div class="setting-field">
                        <div id="read-only-processor-comments"></div>
                    </div>
                    <div class="clear"></div>
                </div>
            </div>
        </div>
    </div>
</div>