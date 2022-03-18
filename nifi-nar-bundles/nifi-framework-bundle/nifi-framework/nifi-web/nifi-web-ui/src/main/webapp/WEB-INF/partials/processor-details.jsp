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
<div id="processor-details" class="hidden large-dialog">
    <div id="processor-details-status-bar"></div>
    <div class="dialog-content">
        <div id="processor-details-tabs" class="tab-container"></div>
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
                        <div id="read-only-processor-type" class="setting-field"></div>
                        <div class="clear"></div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">Bundle</div>
                        <div id="read-only-processor-bundle" class="setting-field"></div>
                        <div class="clear"></div>
                    </div>
                    <div class="setting">
                        <div class="penalty-duration-setting">
                            <div class="setting-name">
                                Penalty duration
                                <div class="fa fa-question-circle" alt="Info" title="The amount of time used when this processor penalizes a FlowFile."></div>
                            </div>
                            <div class="setting-field">
                                <span id="read-only-penalty-duration"></span>
                            </div>
                        </div>
                        <div class="yield-duration-setting">
                            <div class="setting-name">
                                Yield duration
                                <div class="fa fa-question-circle" alt="Info" title="When a processor yields, it will not be scheduled again until this amount of time elapses."></div>
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
                                <div class="fa fa-question-circle" alt="Info" title="The level at which this processor will generate bulletins."></div>
                            </div>
                            <div class="setting-field">
                                <span id="read-only-bulletin-level"></span>
                            </div>
                        </div>
                        <div class="clear"></div>
                    </div>
                </div>
                <div class="spacer">&nbsp;</div>
            </div>
            <div id="details-scheduling-tab-content" class="details-tab">
                <div class="settings-left">
                    <div class="setting">
                        <div class="scheduling-strategy-setting">
                            <div class="setting-name">
                                Scheduling strategy
                                <div class="fa fa-question-circle" alt="Info" title="The strategy used to schedule this processor."></div>
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
                                <div class="fa fa-question-circle" alt="Info" title="The number of tasks that should be concurrently scheduled for this processor."></div>
                            </div>
                            <div class="setting-field">
                                <span id="read-only-concurrently-schedulable-tasks"></span>
                            </div>
                        </div>
                        <div id="read-only-run-schedule" class="scheduling-period-setting">
                            <div class="setting-name">
                                Run schedule
                                <div class="fa fa-question-circle" alt="Info" title="The minimum number of seconds that should elapse between task executions."></div>
                            </div>
                            <div class="setting-field">
                                <span id="read-only-scheduling-period"></span>
                            </div>
                        </div>
                        <div class="clear"></div>
                    </div>
                    <div id="read-only-execution-node-options" class="setting">
                        <div class="execution-node-setting">
                            <div class="setting-name">
                                Execution
                                <div class="fa fa-question-circle" alt="Info" title="The node(s) that this processor will be scheduled to run on."></div>
                            </div>
                            <div class="setting-field">
                                <span id="read-only-execution-node"></span>
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
                            <div class="fa fa-question-circle" alt="Info" title="When scheduled to run, the processor will continue running for up to this duration. A run duration of 0ms will execute once when scheduled."></div>
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
            <div id="details-processor-relationships-tab-content" class="configuration-tab">
                <div class="settings-left">
                    <div class="setting">
                        <div class="setting-name">
                            Automatically terminate / retry relationships
                            <div class="fa fa-question-circle" alt="Info" title="Will automatically terminate and/or retry FlowFiles sent to a given relationship if it is not defined elsewhere. If both terminate and retry are selected, any retry logic will happen first, then auto-termination."></div>
                        </div>
                        <div class="setting-field">
                            <div id="read-only-auto-action-relationship-names"></div>
                        </div>
                    </div>
                </div>
                <div class="settings-right">
                    <div class="details-retry-count-setting setting">
                        <div class="setting-name">
                            Number of Retry Attempts
                            <div class="fa fa-question-circle" alt="Info" title="For relationships set to retry, this number indicates how many times a FlowFile will attempt to reprocess before it is routed elsewhere."></div>
                        </div>
                        <div class="setting-field">
                            <span type="text" id="details-retry-attempt-count" class="small-setting-input"></span>
                        </div>
                    </div>
                    <div class="details-backoff-policy-setting setting">
                        <div class="setting-name">
                            Retry Back Off Policy
                            <div class="fa fa-question-circle" alt="Info" title="Penalize: Retry attempts will occur in time, but the processor will continue to process other FlowFiles.&#013;&#013;Yield: No other FlowFile processing will occur until all retry attempts have been made."></div>
                        </div>
                        <fieldset class="setting-field" disabled>
                            <input type="radio" id="penalizeFlowFile" name="backoffPolicy" value="PENALIZE_FLOWFILE">
                            <label for="penalizeFlowFile">Penalize</label>

                            <input type="radio" class="yield-radio" id="yieldEntireProcessor" name="backoffPolicy" value="YIELD_PROCESSOR">
                            <label for="yieldEntireProcessor">Yield</label>
                        </fieldset>
                    </div>
                    <div class="details-max-backoff-setting setting">
                        <div class="setting-name">
                            Retry Maximum Back Off Period
                            <div class="fa fa-question-circle" alt="Info" title="Initial retries are based on the Penalty/Yield Duration time specified in the Settings tab. The duration time is repeatedly doubled for every subsequent retry attempt. This number indicates the maximum allowable time period before another retry attempt occurs."></div>
                        </div>
                        <div class="setting-field">
                            <span type="text" id="details-max-backoff-period" class="small-setting-input">
                        </div>
                    </div>
                </div>
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
