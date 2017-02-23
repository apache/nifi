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
<div id="controller-service-configuration" class="hidden large-dialog">
    <div class="controller-service-configuration-tab-container dialog-content">
        <div id="controller-service-configuration-tabs" class="tab-container"></div>
        <div id="controller-service-configuration-tabs-content">
            <div id="controller-service-standard-settings-tab-content" class="configuration-tab">
                <div class="settings-left">
                    <div class="setting">
                        <div class="setting-name">Name</div>
                        <div class="controller-service-editable setting-field">
                            <input type="text" id="controller-service-name" name="controller-service-name" class="setting-input"/>
                        </div>
                        <div class="controller-service-read-only setting-field hidden">
                            <span id="read-only-controller-service-name"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">Id</div>
                        <div class="setting-field">
                            <span id="controller-service-id"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">Type</div>
                        <div class="setting-field">
                            <span id="controller-service-type"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">Bundle</div>
                        <div id="controller-service-bundle" class="setting-field"></div>
                    </div>
                    <div class="setting">
                        <div class="setting-name">Supports Controller Service</div>
                        <div id="controller-service-compatible-apis" class="setting-field"></div>
                    </div>
                </div>
                <div class="spacer">&nbsp;</div>
                <div class="settings-right">
                    <div class="setting">
                        <div class="setting-name">
                            Referencing Components
                            <div class="fa fa-question-circle" alt="Info" title="Other components referencing this controller service."></div>
                        </div>
                        <div class="setting-field">
                            <div id="controller-service-referencing-components"></div>
                        </div>
                    </div>
                </div>
            </div>
            <div id="controller-service-properties-tab-content" class="configuration-tab">
                <div id="controller-service-properties"></div>
            </div>
            <div id="controller-service-comments-tab-content" class="configuration-tab">
                <textarea cols="30" rows="4" id="controller-service-comments" name="controller-service-comments" class="controller-service-editable setting-input"></textarea>
                <div class="setting controller-service-read-only hidden">
                    <div class="setting-name">Comments</div>
                    <div class="setting-field">
                        <span id="read-only-controller-service-comments"></span>
                    </div>
                </div>
            </div>
        </div>
    </div>
</div>
<div id="new-controller-service-property-container"></div>