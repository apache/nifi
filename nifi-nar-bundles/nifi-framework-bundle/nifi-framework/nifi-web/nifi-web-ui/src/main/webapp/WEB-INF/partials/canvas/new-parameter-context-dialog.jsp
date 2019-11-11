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
<div id="parameter-context-dialog" layout="column" class="hidden read-only">
    <div id="parameter-context-status-bar"></div>
    <div class="parameter-context-tab-container dialog-content">
        <div id="parameter-context-tabs" class="tab-container"></div>
        <div id="parameter-context-tabs-content">
            <div id="parameter-context-standard-settings-tab-content" class="configuration-tab">
                <div class="settings-left">
                    <div id="parameter-context-id-setting" class="setting hidden">
                        <div class="setting-name">Id</div>
                        <div class="setting-field">
                            <div id="parameter-context-id-field" class="ellipsis"></div>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">Name</div>
                        <div id="parameter-context-name-container" class="setting-field">
                            <input type="text" id="parameter-context-name" class="edit-mode" name="parameter-context-name"/>
                            <div id="parameter-context-name-read-only" class="read-only ellipsis"></div>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">Description</div>
                        <div class="setting-field parameter-context-description-container">
                            <textarea id="parameter-context-description-field" class="edit-mode" rows="6"></textarea>
                            <div id="parameter-context-description-read-only" class="read-only"></div>
                        </div>
                        <div class="clear"></div>
                    </div>
                </div>
                <div class="spacer">&nbsp;</div>
                <div class="settings-right">
                </div>
            </div>
            <div id="parameter-context-parameters-tab-content" class="configuration-tab">
                <div class="settings-left">
                    <div class="edit-mode">
                        <div id="add-parameter"><button class="button fa fa-plus"></button></div>
                        <div class="clear"></div>
                    </div>
                    <div id="parameter-table"></div>
                    <div id="parameter-context-update-status" class="hidden">
                        <div class="setting">
                            <div class="setting-name">
                                Steps to update parameters
                            </div>
                            <div class="setting-field">
                                <ol id="parameter-context-update-steps"></ol>
                            </div>
                        </div>
                    </div>
                </div>
                <div class="spacer">&nbsp;</div>
                <div id="parameter-context-usage" class="settings-right">
                    <div class="setting">
                        <div class="setting-name">
                            Parameter
                        </div>
                        <div class="setting-field">
                            <div id="parameter-referencing-components-context" class="ellipsis"></div>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">
                            Referencing Components
                            <div class="fa fa-question-circle" alt="Info" title="Components referencing this parameter grouped by process group."></div>
                        </div>
                        <div id="parameter-referencing-components-container" class="setting-field">
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
</div>
<div id="parameter-dialog" class="dialog cancellable hidden">
    <div class="dialog-content">
        <div class="setting">
            <div class="setting-name">Name</div>
            <div class="setting-field new-parameter-name-container">
                <input id="parameter-name" type="text"/>
            </div>
            <div class="clear"></div>
        </div>
        <div class="setting">
            <div class="setting-name">
                Value
                <div class="fa fa-question-circle" alt="Info" title="Parameter values do not support Expression Language or embedded parameter references."></div>
            </div>
            <div class="setting-field new-parameter-value-container">
                <textarea id="parameter-value-field"></textarea>
                <div class="string-check-container">
                    <div id="parameter-set-empty-string-field" class="nf-checkbox string-check checkbox-unchecked"></div>
                    <span class="string-check-label nf-checkbox-label">Set empty string</span>
                </div>
            </div>
            <div class="clear"></div>
        </div>
        <div class="setting">
            <div class="setting-field new-parameter-sensitive-value-container">
                <div class="setting-name">Sensitive value</div>
                <input id="parameter-sensitive-radio-button" type="radio" name="sensitive" value="sensitive"/> Yes
                <input id="parameter-not-sensitive-radio-button" type="radio" name="sensitive" value="plain" checked="checked" style="margin-left: 20px;"/> No
            </div>
            <div class="clear"></div>
        </div>
        <div class="setting">
            <div class="setting-name">Description</div>
            <div class="setting-field new-parameter-description-container">
                <textarea id="parameter-description-field" rows="6"></textarea>
            </div>
            <div class="clear"></div>
        </div>
    </div>
    <div id="parameter-context-updating-status">
        <div class='parameter-context-step ajax-loading'></div>
        <div class='status-message ellipsis'>Updating parameter context</div>
    </div>
</div>
<div id="referencing-components-template" class="referencing-components-template hidden clear">
    <div class="setting">
        <div class="setting-name">
            Referencing Processors
        </div>
        <div class="setting-field">
            <ul class="parameter-context-referencing-processors"></ul>
        </div>
    </div>
    <div class="setting">
        <div class="setting-name">
            Referencing Controller Services
        </div>
        <div class="setting-field">
            <ul class="parameter-context-referencing-controller-services"></ul>
        </div>
    </div>
    <div class="setting">
        <div class="setting-name">
            Unauthorized referencing components
        </div>
        <div class="setting-field">
            <ul class="parameter-context-referencing-unauthorized-components"></ul>
        </div>
    </div>
</div>
