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
<div id="history-filter-dialog" class="hidden medium-dialog">
    <div class="dialog-content">
        <div class="setting">
            <div class="setting-name">Filter</div>
            <div class="setting-field">
                <div id="history-filter-controls">
                    <input type="text" id="history-filter" class="history-large-input"/>
                    <div id="history-filter-type"></div>
                    <div class="clear"></div>
                </div>
            </div>
        </div>
        <div class="setting">
            <div class="start-date-setting">
                <div class="setting-name">
                    Start date
                    <div class="fa fa-question-circle" alt="Info" title="The start date in the format 'mm/dd/yyyy'. Must also specify start time."></div>
                </div>
                <div class="setting-field">
                    <input type="text" id="history-filter-start-date" class="history-small-input"/>
                </div>
            </div>
            <div class="start-time-setting">
                <div class="setting-name">
                    Start time (<span class="timezone"></span>)
                    <div class="fa fa-question-circle" alt="Info" title="The start time in the format 'hh:mm:ss'. Must also specify start date."></div>
                </div>
                <div class="setting-field">
                    <input type="text" id="history-filter-start-time" class="history-small-input"/>
                </div>
            </div>
            <div class="clear"></div>
        </div>
        <div class="setting">
            <div class="end-date-setting">
                <div class="setting-name">
                    End date
                    <div class="fa fa-question-circle" alt="Info" title="The end date in the format 'mm/dd/yyyy'. Must also specify end time."></div>
                </div>
                <div class="setting-field">
                    <input type="text" id="history-filter-end-date" class="history-small-input"/>
                </div>
            </div>
            <div class="end-time-setting">
                <div class="setting-name">
                    End time (<span class="timezone"></span>)
                    <div class="fa fa-question-circle" alt="Info" title="The end time in the format 'hh:mm:ss'. Must also specify end date."></div>
                </div>
                <div class="setting-field">
                    <input type="text" id="history-filter-end-time" class="history-small-input"/>
                </div>
            </div>
            <div class="clear"></div>
        </div>
    </div>
</div>
