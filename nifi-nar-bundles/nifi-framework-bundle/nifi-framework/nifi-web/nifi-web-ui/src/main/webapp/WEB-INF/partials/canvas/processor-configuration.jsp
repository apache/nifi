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
<div id="processor-configuration" layout="column" class="hidden large-dialog">
    <div class="processor-configuration-tab-container dialog-content">
        <div id="processor-configuration-tabs" class="tab-container"></div>
        <div id="processor-configuration-tabs-content">
            <div id="processor-standard-settings-tab-content" class="configuration-tab">
                <div class="settings-left">
                    <div class="setting">
                        <div class="setting-name">Name</div>
                        <div id="processor-name-container" class="setting-field">
                            <input type="text" id="processor-name" name="processor-name"/>
                            <div class="processor-enabled-container">
                                <div id="processor-enabled" class="nf-checkbox checkbox-unchecked"></div>
                                <span class="nf-checkbox-label"> Enabled</span>
                            </div>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">Id</div>
                        <div class="setting-field">
                            <span id="processor-id"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">Type</div>
                        <div class="setting-field">
                            <span id="processor-type"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">Bundle</div>
                        <div id="processor-bundle" class="setting-field"></div>
                    </div>
                    <div class="setting">
                        <div class="penalty-duration-setting">
                            <div class="setting-name">
                                Penalty duration
                                <div class="fa fa-question-circle" alt="Info" title="The amount of time used when this processor penalizes a FlowFile."></div>
                            </div>
                            <div class="setting-field">
                                <input type="text" id="penalty-duration" name="penalty-duration" class="small-setting-input"/>
                            </div>
                        </div>
                        <div class="yield-duration-setting">
                            <div class="setting-name">
                                Yield duration
                                <div class="fa fa-question-circle" alt="Info" title="When a processor yields, it will not be scheduled again until this amount of time elapses."></div>
                            </div>
                            <div class="setting-field">
                                <input type="text" id="yield-duration" name="yield-duration" class="small-setting-input"/>
                            </div>
                        </div>
                        <div class="clear"></div>
                    </div>
                    <div class="setting">
                        <div class="bulletin-setting">
                            <div class="setting-name">
                                Bulletin level
                                <div class="fa fa-question-circle" alt="Info" title="The level at which this processor will generate bulletins."></div>
                            </div>
                            <div class="setting-field">
                                <div id="bulletin-level-combo"></div>
                            </div>
                        </div>
                        <div class="clear"></div>
                    </div>
                </div>
                <div class="spacer">&nbsp;</div>
                <div class="settings-right">
                    <div class="setting">
                        <div class="setting-name">
                            Automatically terminate relationships
                            <div class="fa fa-question-circle" alt="Info" title="Will automatically terminate FlowFiles sent to a given relationship if it is not defined elsewhere."></div>
                        </div>
                        <div class="setting-field">
                            <div id="auto-terminate-relationship-names"></div>
                        </div>
                    </div>
                </div>
            </div>
            <div id="processor-scheduling-tab-content" class="configuration-tab">
                <div class="settings-left">
                    <div class="setting">
                        <div class="scheduling-strategy-setting">
                            <div class="setting-name">
                                Scheduling strategy
                                <div class="fa fa-question-circle" alt="Info" title="The strategy used to schedule this processor."></div>
                            </div>
                            <div class="setting-field">
                                <div type="text" id="scheduling-strategy-combo"></div>
                            </div>
                        </div>
                        <div id="event-driven-warning" class="hidden">
                            <div class="processor-configuration-warning-icon"></div>
                            This strategy is experimental
                        </div>
                        <div class="clear"></div>
                    </div>
                    <div id="timer-driven-options" class="setting">
                        <div class="concurrently-schedulable-tasks-setting">
                            <div class="setting-name">
                                Concurrent tasks
                                <div class="fa fa-question-circle" alt="Info" title="The number of tasks that should be concurrently scheduled for this processor."></div>
                            </div>
                            <div class="setting-field">
                                <input type="text" id="timer-driven-concurrently-schedulable-tasks" name="timer-driven-concurrently-schedulable-tasks" class="small-setting-input"/>
                            </div>
                        </div>
                        <div class="scheduling-period-setting">
                            <div class="setting-name">
                                Run schedule
                                <div class="fa fa-question-circle" alt="Info" title="The amount of time that should elapse between task executions."></div>
                            </div>
                            <div class="setting-field">
                                <input type="text" id="timer-driven-scheduling-period" name="timer-driven-scheduling-period" class="small-setting-input"/>
                            </div>
                        </div>
                        <div class="clear"></div>
                    </div>
                    <div id="event-driven-options" class="setting">
                        <div class="concurrently-schedulable-tasks-setting">
                            <div class="setting-name">
                                Concurrent tasks
                                <div class="fa fa-question-circle" alt="Info" title="The number of tasks that should be concurrently scheduled for this processor."></div>
                            </div>
                            <div class="setting-field">
                                <input type="text" id="event-driven-concurrently-schedulable-tasks" name="event-driven-concurrently-schedulable-tasks" class="small-setting-input"/>
                            </div>
                        </div>
                        <div class="clear"></div>
                    </div>
                    <div id="cron-driven-options" class="setting">
                        <div class="concurrently-schedulable-tasks-setting">
                            <div class="setting-name">
                                Concurrent tasks
                                <div class="fa fa-question-circle" alt="Info" title="The number of tasks that should be concurrently scheduled for this processor."></div>
                            </div>
                            <div class="setting-field">
                                <input type="text" id="cron-driven-concurrently-schedulable-tasks" name="cron-driven-concurrently-schedulable-tasks" class="small-setting-input"/>
                            </div>
                        </div>
                        <div class="scheduling-period-setting">
                            <div class="setting-name">
                                Run schedule
                                <div class="fa fa-question-circle" alt="Info" title="The CRON expression that defines when this processor should run."></div>
                            </div>
                            <div class="setting-field">
                                <input type="text" id="cron-driven-scheduling-period" name="cron-driven-scheduling-period" class="small-setting-input"/>
                            </div>
                        </div>
                        <div class="clear"></div>
                    </div>
                    <div id="execution-node-options" class="setting">
                        <div class="execution-node-setting">
                            <div class="setting-name">
                                Execution
                                <div class="fa fa-question-circle" alt="Info" title="The node(s) that this processor will be scheduled to run on."></div>
                            </div>
                            <div class="setting-field">
                                <div id="execution-node-combo"></div>
                            </div>
                        </div>
                        <div class="clear"></div>
                    </div>
                </div>
                <div class="spacer">&nbsp;</div>
                <div id="run-duration-setting-container" class="settings-right">
                    <div class="setting">
                        <div class="setting-name">
                            Run duration
                            <div class="fa fa-question-circle" alt="Info"
                                 title="When scheduled to run, the processor will continue running for up to this duration. A run duration of 0ms will execute once when scheduled."></div>
                        </div>
                        <div class="setting-field" style="overflow: visible;">
                            <div id="run-duration-container">
                                <div id="run-duration-labels">
                                    <div id="run-duration-zero">0ms</div>
                                    <div id="run-duration-one">25ms</div>
                                    <div id="run-duration-two">50ms</div>
                                    <div id="run-duration-three">100ms</div>
                                    <div id="run-duration-four">250ms</div>
                                    <div id="run-duration-five">500ms</div>
                                    <div id="run-duration-six">1s</div>
                                    <div id="run-duration-seven">2s</div>
                                    <div class="clear"></div>
                                </div>
                                <div id="run-duration-slider"></div>
                                <div id="run-duration-explanation">
                                    <div id="min-run-duration-explanation">Lower latency</div>
                                    <div id="max-run-duration-explanation">Higher throughput</div>
                                    <div class="clear"></div>
                                </div>
                                <div id="run-duration-data-loss" class="hidden">
                                    <div class="processor-configuration-warning-icon"></div>
                                    Source Processors with a run duration greater than 0ms and no incoming connections could lose data when NiFi is shutdown.
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            <div id="processor-properties-tab-content" class="configuration-tab">
                <div id="processor-properties"></div>
            </div>
            <div id="processor-comments-tab-content" class="configuration-tab">
                <textarea cols="30" rows="4" id="processor-comments" name="processor-comments" class="setting-input"></textarea>
            </div>
        </div>
    </div>
</div>
<div id="new-processor-property-container"></div>
