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
<div id="disable-controller-service-dialog" layout="column" class="hidden large-dialog">
    <div class="dialog-content">
        <div class="settings-left">
            <div id="disable-controller-service-service-container" class="setting">
                <div class="setting-name">Service</div>
                <div class="setting-field">
                    <span id="disable-controller-service-id" class="hidden"></span>
                    <div id="disable-controller-service-name"></div>
                    <div id="disable-controller-service-bulletins"></div>
                    <div class="clear"></div>
                </div>
            </div>
            <div id="disable-controller-service-scope-container" class="setting">
                <div class="setting-name">Scope</div>
                <div class="setting-field">
                    Service and referencing components
                    <div class="fa fa-question-circle" alt="Info" title="Referencing components must be disabled/stopped in order to disable this service."></div>
                </div>
            </div>
            <div id="disable-controller-service-progress-container" class="setting hidden">
                <div id="disable-progress-label" class="setting-name"></div>
                <div class="setting-field">
                    <ol id="disable-controller-service-progress">
                        <li>
                            Stopping referencing processors and reporting tasks
                            <div id="disable-referencing-schedulable" class="disable-referencing-components"></div>
                            <div class="clear"></div>
                        </li>
                        <li>
                            Disabling referencing controller services
                            <div id="disable-referencing-services" class="disable-referencing-components"></div>
                            <div class="clear"></div>
                        </li>
                        <li>
                            Disabling this controller service
                            <div id="disable-controller-service" class="disable-referencing-components"></div>
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
                    <div class="fa fa-question-circle" alt="Info" title="Other components referencing this controller service."></div>
                </div>
                <div class="setting-field">
                    <div id="disable-controller-service-referencing-components"></div>
                </div>
            </div>
        </div>
    </div>
    <div class="controller-service-canceling hidden unset">
        Canceling...
    </div>
</div>
