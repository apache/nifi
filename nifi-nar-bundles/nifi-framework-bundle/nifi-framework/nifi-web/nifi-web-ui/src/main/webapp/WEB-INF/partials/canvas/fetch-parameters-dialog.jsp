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
<div id="fetch-parameters-dialog" layout="column" class="hidden large-dialog">
    <div class="dialog-content">
        <div class="settings-left">
            <div id="fetch-parameters-container" class="setting">
                <div class="setting-name">Name</div>
                <div class="setting-field">
                    <span id="fetch-parameters-id" class="hidden"></span>
                    <div id="fetch-parameters-name"></div>
                    <div id="fetch-parameters-bulletins"></div>
                    <div class="clear"></div>
                </div>
            </div>
            <div id="fetch-parameters-usage-container" class="setting">
                <div id="fetch-parameters-usage" class="settings-right">
                    <div class="setting">
                        <div class="setting-name">
                            Parameters
                            <div class="referencing-components-loading"></div>
                        </div>
                        <div class="setting-field">
                            <ol id="fetch-parameters-listing"></ol>
                        </div>
                    </div>
                </div>
            </div>
            <div id="fetch-parameters-update-status-container" class="setting">
                <div id="fetch-parameters-update-status" class="hidden">
                    <div class="setting">
                        <div class="setting-name">
                            Steps to update parameters
                        </div>
                        <div class="setting-field">
                            <ol id="fetch-parameters-update-steps"></ol>
                        </div>
                    </div>
                </div>
            </div>
            <div id="fetch-parameters-progress-container" class="setting hidden">
                <div id="fetch-parameters-progress-label" class="setting-name"></div>
                <div class="setting-field">
                    <ol id="fetch-parameters-progress">
                        <li>
                            Fetching the parameters
                            <div id="fetch-parameters" class="fetch-parameters-referencing-components"></div>
                            <div class="clear"></div>
                        </li>
                    </ol>
                </div>
            </div>
        </div>
        <div class="spacer">&nbsp;</div>
        <div class="settings-right">
            <div class="setting">
                <div class="setting-name">
                    Referencing Components
                    <div class="fa fa-question-circle" alt="Info" title="Other components referencing this Parameter Provider."></div>
                </div>
                <div class="setting-field">
                    <div id="fetch-parameters-referencing-components"></div>
                </div>
            </div>
        </div>
    </div>
    <div class="fetch-parameters-canceling hidden unset">
        Canceling...
    </div>
</div>