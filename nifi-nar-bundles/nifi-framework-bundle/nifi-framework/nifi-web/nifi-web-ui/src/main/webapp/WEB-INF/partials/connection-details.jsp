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
<div id="connection-details">
    <div class="connection-details-tab-container dialog-content">
        <div id="connection-details-tabs" class="tab-container"></div>
        <div id="connection-details-tabs-content">
            <div id="read-only-connection-details-tab-content" class="configuration-tab">
                <div class="settings-left">
                    <div class="setting">
                        <div id="read-only-connection-source-label" class="setting-name"></div>
                        <div class="setting-field connection-terminal-label">
                            <div id="read-only-connection-source" class="ellipsis"></div>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">Within group</div>
                        <div class="setting-field">
                            <div id="read-only-connection-source-group-name"></div>
                        </div>
                    </div>
                    <div id="read-only-relationship-names-container" class="setting">
                        <div class="setting-name">
                            Relationships
                            <div class="fa fa-question-circle" alt="Info" title="Selected relationships are in bold."></div>
                        </div>
                        <div class="setting-field">
                            <div id="read-only-relationship-names"></div>
                        </div>
                    </div>
                </div>
                <div class="spacer">&nbsp;</div>
                <div class="settings-right">
                    <div class="setting">
                        <div id="read-only-connection-target-label" class="setting-name"></div>
                        <div class="setting-field connection-terminal-label">
                            <div id="read-only-connection-target" class="ellipsis"></div>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">Within group</div>
                        <div class="setting-field">
                            <div id="read-only-connection-target-group-name"></div>
                        </div>
                    </div>
                </div>
            </div>
            <div id="read-only-connection-settings-tab-content" class="configuration-tab">
                <div class="settings-left">
                    <div class="setting">
                        <div class="setting-name">Name</div>
                        <div class="setting-field">
                            <span id="read-only-connection-name"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">Id</div>
                        <div class="setting-field">
                            <span id="read-only-connection-id"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">
                            FlowFile expiration
                            <div class="fa fa-question-circle" alt="Info" title="The maximum amount of time an object may be in the flow before it will be automatically aged out of the flow."></div>
                        </div>
                        <div class="setting-field">
                            <span id="read-only-flow-file-expiration"></span>
                        </div>
                        <div class="clear"></div>
                    </div>
                    <div class="multi-column-settings">
                        <div class="setting">
                            <div class="setting-name">
                                Back Pressure<br/>Object threshold
                                <div class="fa fa-question-circle" alt="Info" title="The maximum number of objects that can be queued before back pressure is applied."></div>
                            </div>
                            <div class="setting-field">
                                <span id="read-only-back-pressure-object-threshold"></span>
                            </div>
                            <div class="clear"></div>
                        </div>
                        <div class="separator">&nbsp;</div>
                        <div class="setting">
                            <div class="setting-name">
                                &nbsp;<br/>Size threshold
                                <div class="fa fa-question-circle" alt="Info" title="The maximum data size of objects that can be queued before back pressure is applied."></div>
                            </div>
                            <div class="setting-field">
                                <span id="read-only-back-pressure-data-size-threshold"></span>
                            </div>
                            <div class="clear"></div>
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
                                    <div id="read-only-load-balance-strategy"></div>
                                </div>
                            </div>
                            <div class="separator">&nbsp;</div>
                            <div id="read-only-load-balance-partition-attribute-setting" class="setting">
                                <div class="setting-name">
                                    Attribute Name
                                    <div class="fa fa-question-circle" alt="Info" title="The FlowFile Attribute to use for determining which node a FlowFile will go to."></div>
                                </div>
                                <div class="setting-field">
                                    <span id="read-only-load-balance-partition-attribute"></span>
                                </div>
                            </div>
                        </div>
                        <div id="read-only-load-balance-compression-setting" class="setting">
                            <div class="setting-name">
                                Load Balance Compression
                                <div class="fa fa-question-circle" alt="Info" title="Whether or not data should be compressed when being transferred between nodes in the cluster."></div>
                            </div>
                            <div class="setting-field">
                                <div id="read-only-load-balance-compression"></div>
                            </div>
                        </div>
                    </div>
                </div>
                <div class="spacer">&nbsp;</div>
                <div class="settings-right">
                    <div class="setting">
                        <div class="setting-name">
                            Prioritizers
                            <div class="fa fa-question-circle" alt="Info" title="Prioritizers that have been selected to reprioritize FlowFiles in this processors work queue."></div>
                        </div>
                        <div class="setting-field">
                            <div id="read-only-prioritizers"></div>
                        </div>
                        <div class="clear"></div>
                    </div>
                </div>
            </div>
        </div>
    </div>
</div>