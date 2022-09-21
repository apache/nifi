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
    <div id="fetch-parameters-status-bar"></div>
    <div class="dialog-content">
        <%--settings-left--%>
        <div class="settings-left">
            <div id="fetch-parameters-provider-groups-container" class="setting">
                <div class="setting-name">Name</div>
                <div class="setting-field">
                    <span id="fetch-parameters-id" class="hidden"></span>
                    <div id="fetch-parameters-name"></div>
                    <div class="clear"></div>
                </div>
            </div>
            <div id="fetch-parameters-usage-container" class="setting">
                <div class="setting-name">
                    Select groups to create parameter contexts
                    <div class="fa fa-question-circle" alt="Info" title="Select a parameter group to cconfigure as a parameter context."></div>
                </div>
                <div id="parameter-groups-table"></div>
            </div>
            <div id="apply-groups-container" class="setting hidden">
                <div class="setting-name">Parameter Groups</div>
                <div class="setting-field">
                    <div id="apply-groups-list"></div>
                </div>
            </div>
        </div>
        <%--end settings-left--%>

        <div class="spacer">&nbsp;</div>

        <%--settings-center--%>
        <div class="settings-center">
            <div id="parameters-container" class="setting">
                <div id="create-parameter-context-checkbox-container" class="setting-field"></div>
                <div id="fetched-parameters-container" class="setting">
                    <div class="setting-name">
                        Fetched parameters
                        <div class="fa fa-question-circle" alt="Info" title="Fetched parameters for the selected parameter group."></div>
                    </div>
                    <div id="fetched-parameters-listing-container" class="setting-field">
                        <ol id="fetched-parameters-listing"></ol>
                    </div>
                </div>
                <div id="selectable-parameters-container" class="setting">
                    <div class="setting-name">
                        Select parameters to be marked as sensitive
                        <div class="fa fa-question-circle" alt="Info" title="Selected parameters will be set as sensitive."></div>
                    </div>
                    <div id="selectable-parameters-buttons">
                        <div id="select-all-fetched-parameters" class="secondary-button fa fa-check-square-o button-icon hidden"><span>Select all</span></div>
                        <div id="deselect-all-fetched-parameters" class="secondary-button fa fa-minus-square-o button-icon hidden"><span>Deselect all</span></div>
                        <div class="clear"></div>
                    </div>
                    <div id="selectable-parameters-table" class="setting-field"></div>
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
        </div>
        <%--end settings-center--%>

        <div class="spacer">&nbsp;</div>

        <%--settings-right--%>
        <div class="settings-right">
            <div class="setting">
                <div class="setting-name">
                    Parameter Contexts To Create
                    <div class="fa fa-question-circle" alt="Info" title="Parameter groups set to be created as parameter contexts."></div>
                </div>
                <div class="setting-field">
                    <div id="parameter-contexts-to-create-container" class="ellipsis"></div>
                </div>
            </div>
            <div class="setting">
                <div class="setting-name">
                    Parameter Contexts To Update
                    <div class="fa fa-question-circle" alt="Info" title="Parameter Contexts to be updated."></div>
                    <div class="referencing-components-loading"></div>
                </div>
                <div class="setting-field">
                    <div id="parameter-contexts-to-update-container" class="ellipsis"></div>
                </div>
            </div>

            <div id="fetch-parameters-referencing-components-container" class="setting hidden">
                <div class="setting-name">
                    Referencing Components
                    <div class="fa fa-question-circle" alt="Info" title="Components referencing this parameter."></div>
                    <div class="referencing-components-loading"></div>
                </div>
                <div id="fetch-parameter-referencing-components-container" class="setting-field">
                </div>
            </div>
            <div id="fetch-parameters-referencing-components-template" class="fetch-parameters-referencing-components-template hidden clear">
                <div class="setting">
                    <div class="setting-name">Referencing Processors
                    </div>
                    <div class="setting-field">
                        <ul class="fetch-parameters-referencing-processors"></ul>
                    </div>
                </div>
                <div class="setting">
                    <div class="setting-name">
                        Referencing Controller Services
                    </div>
                    <div class="setting-field">
                        <ul class="fetch-parameters-referencing-controller-services"></ul>
                    </div>
                </div>
                <div class="setting">
                    <div class="setting-name">
                        Unauthorized referencing components
                    </div>
                    <div class="setting-field">
                        <ul class="fetch-parameters-referencing-unauthorized-components"></ul>
                    </div>
                </div>
            </div>

            <div id="fetch-parameters-affected-referencing-components-container" class="setting">
                <div class="setting-name">
                    Affected Referencing Components
                    <div class="fa fa-question-circle" alt="Info" title="Affected components referencing this parameter provider."></div>
                    <div class="referencing-components-loading"></div>
                </div>
                <div id="affected-referencing-components-container" class="setting-field">
                </div>
            </div>
            <div id="affected-referencing-components-template" class="affected-referencing-components-template hidden clear">
                <div class="setting">
                    <div class="setting-name">Referencing Processors
                    </div>
                    <div class="setting-field">
                        <ul class="fetch-parameters-referencing-processors"></ul>
                    </div>
                </div>
                <div class="setting">
                    <div class="setting-name">
                        Referencing Controller Services
                    </div>
                    <div class="setting-field">
                        <ul class="fetch-parameters-referencing-controller-services"></ul>
                    </div>
                </div>
                <div class="setting">
                    <div class="setting-name">
                        Unauthorized referencing components
                    </div>
                    <div class="setting-field">
                        <ul class="fetch-parameters-referencing-unauthorized-components"></ul>
                    </div>
                </div>
            </div>
        </div>
    </div>
    <%--end settings-right--%>

    <div class="fetch-parameters-canceling hidden unset">
        Canceling...
    </div>
</div>
