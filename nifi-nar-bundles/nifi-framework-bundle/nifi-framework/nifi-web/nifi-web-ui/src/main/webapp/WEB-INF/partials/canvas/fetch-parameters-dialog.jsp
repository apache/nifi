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
                    Select to configure a parameter group
                    <div class="fa fa-question-circle" alt="Info" title="Discovered parameter groups from this parameter provider. Select a group to create a parameter context, then configure its parameter sensitivities."></div>
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
                        <div class="fa fa-question-circle" alt="Info" title="Discovered parameters from the selected parameter group."></div>
                    </div>
                    <div id="fetched-parameters-listing-container" class="setting-field">
                        <ol id="fetched-parameters-listing"></ol>
                    </div>
                </div>
                <div id="selectable-parameters-container" class="setting">
                    <div class="setting-name">
                        Select parameters to be set as sensitive
                        <div class="fa fa-question-circle" alt="Info" title="Only parameters that are not referenced can be modified."></div>
                    </div>
                    <div id="selectable-parameters-buttons">
                        <button id="select-all-fetched-parameters" class="selectable-parameters-buttons">
                            <div class="fa fa-check-square-o"></div>
                            <span>Select all</span>
                        </button>
                        <button id="deselect-all-fetched-parameters" class="selectable-parameters-buttons">
                            <div class="fa fa-minus-square-o"></div>
                            <span>Deselect all</span>
                        </button>
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
                    <div class="fa fa-question-circle" alt="Info" title="Parameter groups set to be created as parameter contexts, pending apply."></div>
                </div>
                <div class="setting-field">
                    <div id="parameter-contexts-to-create-container" class="ellipsis"></div>
                </div>
            </div>
            <div class="setting">
                <div class="setting-name">
                    Parameter Contexts To Update
                    <div class="fa fa-question-circle" alt="Info" title="Synced parameter contexts to be updated, pending apply."></div>
                    <div class="referencing-components-loading"></div>
                </div>
                <div class="setting-field">
                    <div id="parameter-contexts-to-update-container" class="ellipsis"></div>
                </div>
            </div>

            <div id="fetch-parameters-referencing-components-container" class="setting hidden">
                <div class="setting-name">
                    Referencing Components
                    <div class="fa fa-question-circle" alt="Info" title="Components referencing this selected parameter."></div>
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
    <div id="fetch-parameters-permissions-parameter-contexts-message" class="ellipsis fetch-parameters-dialog-message hidden">
        You do not have permissions to modify one or more synced parameter contexts.
    </div>
    <div id="fetch-parameters-permissions-affected-components-message" class="ellipsis fetch-parameters-dialog-message hidden">
        You do not have permissions to modify one or more affected components.
    </div>
    <div id="fetch-parameters-missing-context-name-message" class="ellipsis fetch-parameters-dialog-message hidden">
        Missing parameter context name.
    </div>
    <div class="fetch-parameters-canceling hidden unset">
        Canceling...
    </div>
</div>
