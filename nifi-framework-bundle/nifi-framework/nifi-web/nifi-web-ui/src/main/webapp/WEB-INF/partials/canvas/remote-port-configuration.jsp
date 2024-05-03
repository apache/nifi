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
<div id="remote-port-configuration" class="hidden medium-dialog">
    <div class="dialog-content">
        <div class="setting">
            <div class="setting-name">Name</div>
            <div class="setting-field">
                <span id="remote-port-id" class="hidden"></span>
                <span id="remote-port-type" class="hidden"></span>
                <div id="remote-port-name" class="ellipsis"></div>
            </div>
        </div>
        <div class="setting">
            <div class="setting-name">
                Concurrent tasks
                <div class="fa fa-question-circle" alt="Info" title="The number of tasks that should be concurrently scheduled for this port."></div>
            </div>
            <div class="setting-field">
                <input id="remote-port-concurrent-tasks" type="text"/>
                <div id="remote-port-use-compression-container">
                    <div id="remote-port-use-compression" class="nf-checkbox"></div>
                    <span class="nf-checkbox-label">Compressed</span>
                </div>
            </div>
            <div class="clear"></div>
        </div>
        <div class="batch-settings">
            <div class="setting-name">
                Batch Settings:
            </div>
            <div class="setting batch-setting">
                <div class="setting-name">
                    Count
                    <div class="fa fa-question-circle" alt="Info" title="The preferred number of flow files to include in a transaction for this port."></div>
                </div>
                <div class="setting-field">
                    <input id="remote-port-batch-count" type="text"/>
                </div>
            </div>
            <div class="setting batch-setting">
                <div class="setting-name">
                    Size
                    <div class="fa fa-question-circle" alt="Info" title="The preferred number of bytes to include in a transaction for this port."></div>
                </div>
                <div class="setting-field">
                    <input id="remote-port-batch-size" type="text"/>
                </div>
            </div>
            <div class="setting batch-setting">
                <div class="setting-name">
                    Duration
                    <div class="fa fa-question-circle" alt="Info" title="The preferred amount of time that a transaction should span for this port."></div>
                </div>
                <div class="setting-field">
                    <input id="remote-port-batch-duration" type="text"/>
                </div>
            </div>
            <div class="clear"></div>
        </div>
    </div>
</div>
