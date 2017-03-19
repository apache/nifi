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
<%@ taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt" %>
<div id="processor-configuration" layout="column" class="hidden large-dialog">
    <div class="processor-configuration-tab-container dialog-content">
        <div id="processor-configuration-tabs" class="tab-container"></div>
        <div id="processor-configuration-tabs-content">
            <div id="processor-standard-settings-tab-content" class="configuration-tab">
                <div class="settings-left">
                    <div class="setting">
                        <div class="setting-name"><fmt:message key="partials.canvas.processor-configuration.Name"/></div>
                        <div id="processor-name-container" class="setting-field">
                            <input type="text" id="processor-name" name="processor-name"/>
                            <div class="processor-enabled-container">
                                <div id="processor-enabled" class="nf-checkbox checkbox-unchecked"></div>
                                <span class="nf-checkbox-label"> <fmt:message key="partials.canvas.processor-configuration.Enabled"/></span>
                            </div>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name"><fmt:message key="partials.canvas.processor-configuration.Id"/></div>
                        <div class="setting-field">
                            <span id="processor-id"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name"><fmt:message key="partials.canvas.processor-configuration.Type"/></div>
                        <div class="setting-field">
                            <span id="processor-type"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="penalty-duration-setting">
                            <div class="setting-name">
                                <fmt:message key="partials.canvas.processor-configuration.PenaltyDuration"/>
                                <fmt:message key="partials.canvas.processor-configuration.PenaltyDuration.title" var="title_PenaltyDuration"/>
                                <div class="fa fa-question-circle" alt="Info" title="${title_PenaltyDuration}"></div>
                            </div>
                            <div class="setting-field">
                                <input type="text" id="penalty-duration" name="penalty-duration" class="small-setting-input"/>
                            </div>
                        </div>
                        <div class="yield-duration-setting">
                            <div class="setting-name">
                                <fmt:message key="partials.canvas.processor-configuration.YieldDuration"/>
                                <fmt:message key="partials.canvas.processor-configuration.YieldDuration.title" var="title_YieldDuration"/>
                                <div class="fa fa-question-circle" alt="Info" title="${title_YieldDuration}"></div>
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
                                <fmt:message key="partials.canvas.processor-configuration.BulletinLevel"/>
                                <fmt:message key="partials.canvas.processor-configuration.BulletinLevel.title" var="title_BulletinLevel"/>
                                <div class="fa fa-question-circle" alt="Info" title="${title_BulletinLevel}"></div>
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
                            <fmt:message key="partials.canvas.processor-configuration.AutoTerminateRelationships"/>
                            <fmt:message key="partials.canvas.processor-configuration.AutoTerminateRelationships.title" var="title_AutoTerminateRelationships"/>
                            <div class="fa fa-question-circle" alt="Info" title="${title_AutoTerminateRelationships}"></div>
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
                                <fmt:message key="partials.canvas.processor-configuration.SchedulingStrategy"/>
                                <fmt:message key="partials.canvas.processor-configuration.SchedulingStrategy.title" var="title_SchedulingStrategy"/>
                                <div class="fa fa-question-circle" alt="Info" title="${title_SchedulingStrategy}"></div>
                            </div>
                            <div class="setting-field">
                                <div type="text" id="scheduling-strategy-combo"></div>
                            </div>
                        </div>
                        <div id="event-driven-warning" class="hidden">
                            <div class="processor-configuration-warning-icon"></div>
                            <fmt:message key="partials.canvas.processor-configuration.event-driven-warning"/>
                        </div>
                        <div class="clear"></div>
                    </div>
                    <div id="timer-driven-options" class="setting">
                        <div class="concurrently-schedulable-tasks-setting">
                            <div class="setting-name">
                                <fmt:message key="partials.canvas.processor-configuration.ConcurrentTasks"/>
                                <fmt:message key="partials.canvas.processor-configuration.ConcurrentTasks.title" var="title_ConcurrentTasks"/>
                                <div class="fa fa-question-circle" alt="Info" title="${title_ConcurrentTasks}"></div>
                            </div>
                            <div class="setting-field">
                                <input type="text" id="timer-driven-concurrently-schedulable-tasks" name="timer-driven-concurrently-schedulable-tasks" class="small-setting-input"/>
                            </div>
                        </div>
                        <div class="scheduling-period-setting">
                            <div class="setting-name">
                                <fmt:message key="partials.canvas.processor-configuration.RunSchedule"/>
                                <fmt:message key="partials.canvas.processor-configuration.RunSchedule.title.1" var="RunSchedule1"/>
                                <div class="fa fa-question-circle" alt="Info" title="${RunSchedule1}"></div>
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
                                <fmt:message key="partials.canvas.processor-configuration.ConcurrentTasks"/>
                                <fmt:message key="partials.canvas.processor-configuration.ConcurrentTasks.title" var="title_ConcurrentTasks"/>
                                <div class="fa fa-question-circle" alt="Info" title="${title_ConcurrentTasks}"></div>
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
                                <fmt:message key="partials.canvas.processor-configuration.ConcurrentTasks"/>
                                <fmt:message key="partials.canvas.processor-configuration.ConcurrentTasks.title" var="title_ConcurrentTasks"/>
                                <div class="fa fa-question-circle" alt="Info" title="${title_ConcurrentTasks}"></div>
                            </div>
                            <div class="setting-field">
                                <input type="text" id="cron-driven-concurrently-schedulable-tasks" name="cron-driven-concurrently-schedulable-tasks" class="small-setting-input"/>
                            </div>
                        </div>
                        <div class="scheduling-period-setting">
                            <div class="setting-name">
                                <fmt:message key="partials.canvas.processor-configuration.RunSchedule"/>
                                <fmt:message key="partials.canvas.processor-configuration.RunSchedule.title.2" var="RunSchedule2"/>
                                <div class="fa fa-question-circle" alt="Info" title="${RunSchedule2}"></div>
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
                                <fmt:message key="partials.canvas.processor-configuration.Execution"/>
                                <fmt:message key="partials.canvas.processor-configuration.ExecutionTitle" var="ExecutionTitle"/>
                                <div class="fa fa-question-circle" alt="Info" title="${ExecutionTitle}"></div>
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
                            <fmt:message key="partials.canvas.processor-configuration.RunDuration"/>
                            <fmt:message key="partials.canvas.processor-configuration.RunDuration.title" var="title_RunDuration"/>
                            <div class="fa fa-question-circle" alt="Info"
                                 title="${title_RunDuration}"></div>
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
                                    <div id="min-run-duration-explanation"><fmt:message key="partials.canvas.processor-configuration.LowerLatency"/></div>
                                    <div id="max-run-duration-explanation"><fmt:message key="partials.canvas.processor-configuration.HigherThroughput"/></div>
                                    <div class="clear"></div>
                                </div>
                                <div id="run-duration-data-loss" class="hidden">
                                    <div class="processor-configuration-warning-icon"></div>
                                    <fmt:message key="partials.canvas.processor-configuration.HigherThroughput.Warning"/>
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