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
<div id="connection-configuration" layout="column" class="hidden large-dialog">
    <div class="connection-configuration-tab-container dialog-content">
        <div id="connection-configuration-tabs" class="tab-container"></div>
        <div id="connection-configuration-tabs-content">
            <div id="connection-settings-tab-content" class="configuration-tab">
                <div class="settings-left">
                    <div class="setting">
                        <div class="setting-name">Name</div>
                        <div class="setting-field">
                            <input type="text" id="connection-name" name="connection-name" class="setting-input"/>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">Id</div>
                        <div class="setting-field">
                            <span type="text" id="connection-id"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">
                            FlowFile expiration
                            <div class="fa fa-question-circle" alt="Info" title="The maximum amount of time an object may be in the flow before it will be automatically aged out of the flow."></div>
                        </div>
                        <div class="setting-field">
                            <input type="text" id="flow-file-expiration" name="flow-file-expiration" class="setting-input"/>
                        </div>
                    </div>
                    <div class="multi-column-settings">
                        <div class="setting">
                            <div class="setting-name">
                                Back Pressure<br/>Object threshold
                                <div class="fa fa-question-circle" alt="Info" title="The maximum number of objects that can be queued before back pressure is applied."></div>
                            </div>
                            <div class="setting-field">
                                <input type="text" id="back-pressure-object-threshold" name="back-pressure-object-threshold" class="setting-input"/>
                            </div>
                        </div>
                        <div class="separator">&nbsp;</div>
                        <div class="setting">
                            <div class="setting-name">
                                &nbsp;<br/>Size threshold
                                <div class="fa fa-question-circle" alt="Info" title="The maximum data size of objects that can be queued before back pressure is applied."></div>
                            </div>
                            <div class="setting-field">
                                <input type="text" id="back-pressure-data-size-threshold" name="back-pressure-data-size-threshold" class="setting-input"/>
                            </div>
                        </div>
                    </div>
                    <div>
                        <div class="multi-column-settings">
                            <div class="setting">
                                <div class="setting-name">
                                    Load Balance Strategy
                                    <div class="fa fa-question-circle" alt="Info" title="How to load balance the data in this Connection across the nodes in the cluster."></div>
                                </div>
                                <div class="setting-field">
                                    <div id="load-balance-strategy-combo"></div>
                                </div>
                            </div>
                            <div id="load-balance-partition-attribute-setting-separator" class="separator">&nbsp;</div>
                            <div id="load-balance-partition-attribute-setting" class="setting">
                                <div class="setting-name">
                                    Attribute Name
                                    <div class="fa fa-question-circle" alt="Info" title="The FlowFile Attribute to use for determining which node a FlowFile will go to."></div>
                                </div>
                                <div class="setting-field">
                                    <input type="text" id="load-balance-partition-attribute" name="load-balance-partition-attribute" class="setting-input"/>
                                </div>
                            </div>
                        </div>
                        <div id="load-balance-compression-setting" class="setting">
                            <div class="setting-name">
                                Load Balance Compression
                                <div class="fa fa-question-circle" alt="Info" title="Whether or not data should be compressed when being transferred between nodes in the cluster."></div>
                            </div>
                            <div class="setting-field">
                                <div id="load-balance-compression-combo"></div>
                            </div>
                        </div>
                    </div>
                </div>
                <div class="spacer">&nbsp;</div>
                <div class="settings-right">
                    <div class="setting">
                        <div class="setting-name">
                            Available prioritizers
                            <div class="fa fa-question-circle" alt="Info" title="Available prioritizers that could reprioritize FlowFiles in this work queue."></div>
                        </div>
                        <div class="setting-field">
                            <ul id="prioritizer-available"></ul>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">
                            Selected prioritizers
                            <div class="fa fa-question-circle" alt="Info" title="Prioritizers that have been selected to reprioritize FlowFiles in this work queue."></div>
                        </div>
                        <div class="setting-field">
                            <ul id="prioritizer-selected"></ul>
                        </div>
                    </div>
                </div>
                <input type="hidden" id="connection-uri" name="connection-uri"/>
                <input type="hidden" id="connection-source-component-id" name="connection-source-component-id"/>
                <input type="hidden" id="connection-source-id" name="connection-source-id"/>
                <input type="hidden" id="connection-source-group-id" name="connection-source-group-id"/>
                <input type="hidden" id="connection-destination-component-id" name="connection-destination-component-id"/>
                <input type="hidden" id="connection-destination-id" name="connection-destination-id"/>
                <input type="hidden" id="connection-destination-group-id" name="connection-destination-group-id"/>
            </div>
            <div id="connection-details-tab-content" class="configuration-tab">
                <div class="settings-left">
                    <div id="read-only-output-port-source" class="setting hidden">
                        <div class="setting-name">From output</div>
                        <div class="setting-field connection-terminal-label">
                            <div id="read-only-output-port-name" class="ellipsis"></div>
                        </div>
                    </div>
                    <div id="output-port-source" class="setting hidden">
                        <div class="setting-name">From output</div>
                        <div class="setting-field connection-terminal-label">
                            <div id="output-port-options"></div>
                        </div>
                    </div>
                    <div id="input-port-source" class="setting hidden">
                        <div class="setting-name">From input</div>
                        <div class="setting-field connection-terminal-label">
                            <div id="input-port-source-name" class="label ellipsis"></div>
                        </div>
                    </div>
                    <div id="funnel-source" class="setting hidden">
                        <div class="setting-name">From funnel</div>
                        <div class="setting-field connection-terminal-label">
                            <div id="funnel-source-name" class="label ellipsis" title="funnel">funnel</div>
                        </div>
                    </div>
                    <div id="processor-source" class="setting hidden">
                        <div class="setting-name">From processor</div>
                        <div class="setting-field connection-terminal-label">
                            <div id="processor-source-details">
                                <div id="processor-source-name" class="label ellipsis"></div>
                                <div id="processor-source-type" class="ellipsis"></div>
                            </div>
                        </div>
                    </div>
                    <div id="connection-source-group" class="setting">
                        <div class="setting-name">Within group</div>
                        <div class="setting-field">
                            <div id="connection-source-group-name"></div>
                        </div>
                    </div>
                    <div id="relationship-names-container" class="hidden">
                        <div class="setting-name">For relationships</div>
                        <div class="setting-field">
                            <div id="relationship-names"></div>
                        </div>
                    </div>
                </div>
                <div class="spacer">&nbsp;</div>
                <div class="settings-right">
                    <div id="input-port-destination" class="setting hidden">
                        <div class="setting-name">To input</div>
                        <div class="setting-field connection-terminal-label">
                            <div id="input-port-options"></div>
                        </div>
                    </div>
                    <div id="output-port-destination" class="setting hidden">
                        <div class="setting-name">To output</div>
                        <div class="setting-field connection-terminal-label">
                            <div id="output-port-destination-name" class="label ellipsis"></div>
                        </div>
                    </div>
                    <div id="funnel-destination" class="setting hidden">
                        <div class="setting-name">To funnel</div>
                        <div class="setting-field connection-terminal-label">
                            <div id="funnel-destination-name" class="label ellipsis" title="funnel">funnel</div>
                        </div>
                    </div>
                    <div id="processor-destination" class="setting hidden">
                        <div class="setting-name">To processor</div>
                        <div class="setting-field connection-terminal-label">
                            <div id="processor-destination-details">
                                <div id="processor-destination-name" class="label ellipsis"></div>
                                <div id="processor-destination-type" class="ellipsis"></div>
                            </div>
                        </div>
                    </div>
                    <div id="connection-destination-group" class="setting">
                        <div class="setting-name">Within group</div>
                        <div class="setting-field">
                            <div id="connection-destination-group-name"></div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
</div>