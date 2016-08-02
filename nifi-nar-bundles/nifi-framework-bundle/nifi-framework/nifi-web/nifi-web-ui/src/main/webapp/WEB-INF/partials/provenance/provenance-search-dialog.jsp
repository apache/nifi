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
<div id="provenance-search-dialog" class="hidden medium-dialog">
    <div class="dialog-content">
        <div class="setting">
            <div class="setting-name">Fields</div>
            <div class="setting-field">
                <div id="searchable-fields-container">
                    <div id="no-searchable-fields" class="unset">No searchable fields have been configured.</div>
                </div>
            </div>
        </div>
        <div class="setting">
            <div class="start-date-setting">
                <div class="setting-name">
                    Start date
                    <div class="fa fa-question-circle" title="The start date in the format 'mm/dd/yyyy'. Must also specify start time."></div>
                </div>
                <div class="setting-field">
                    <input type="text" id="provenance-search-start-date" class="provenance-small-input"/>
                </div>
            </div>
            <div class="start-time-setting">
                <div class="setting-name">
                    Start time (<span class="timezone"></span>)
                    <div class="fa fa-question-circle" title="The start time in the format 'hh:mm:ss'. Must also specify start date."></div>
                </div>
                <div class="setting-field">
                    <input type="text" id="provenance-search-start-time" class="provenance-time-input"/>
                </div>
            </div>
            <div class="clear"></div>
        </div>
        <div class="setting">
            <div class="end-date-setting">
                <div class="setting-name">
                    End date
                    <div class="fa fa-question-circle" title="The end date in the format 'mm/dd/yyyy'. Must also specify end time."></div>
                </div>
                <div class="setting-field">
                    <input type="text" id="provenance-search-end-date" class="provenance-small-input"/>
                </div>
            </div>
            <div class="end-time-setting">
                <div class="setting-name">
                    End time (<span class="timezone"></span>)
                    <div class="fa fa-question-circle" title="The end time in the format 'hh:mm:ss'. Must also specify end date."></div>
                </div>
                <div class="setting-field">
                    <input type="text" id="provenance-search-end-time" class="provenance-time-input"/>
                </div>
            </div>
            <div class="clear"></div>
        </div>
        <div class="setting">
            <div class="end-date-setting">
                <div class="setting-name">
                    Minimum file size
                    <div class="fa fa-question-circle" title="The minimum file size (e.g. 2 KB)."></div>
                </div>
                <div class="setting-field">
                    <input type="text" id="provenance-search-minimum-file-size" class="provenance-small-input"/>
                </div>
            </div>
            <div class="end-time-setting">
                <div class="setting-name">
                    Maximum file size
                    <div class="fa fa-question-circle" title="The maximum file size (e.g. 4 GB)."></div>
                </div>
                <div class="setting-field">
                    <input type="text" id="provenance-search-maximum-file-size" class="provenance-time-input"/>
                </div>
            </div>
            <div class="clear"></div>
        </div>
        <div id="provenance-search-location-container" class="setting">
            <div class="setting-name">
                Search location
            </div>
            <div class="setting-field">
                <div id="provenance-search-location"></div>
            </div>
            <div class="clear"></div>
        </div>
    </div>
</div>
