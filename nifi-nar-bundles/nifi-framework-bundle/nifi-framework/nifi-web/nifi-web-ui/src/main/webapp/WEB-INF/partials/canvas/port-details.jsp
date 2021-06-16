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
<div id="port-details" class="hidden medium-dialog">
    <div class="dialog-content">
        <div class="port-setting">
            <div class="setting-name">Port name</div>
            <div class="setting-field">
                <span id="read-only-port-name"></span>
            </div>
        </div>
        <div class="port-setting">
            <div class="setting-name">Id</div>
            <div class="setting-field">
                <span id="read-only-port-id"></span>
            </div>
        </div>
        <div class="port-setting">
            <div class="setting-name">Allow Remote Access
                <div class="fa fa-question-circle" alt="Info" title="Whether this port can be accessed as a RemoteGroupPort via Site-to-Site protocol."></div>
            </div>
            <div class="setting-field">
                <span id="read-only-port-allow-remote-access"></span>
            </div>
        </div>
        <div class="port-setting">
            <div class="setting-name">Concurrent Tasks
                <div class="fa fa-question-circle" alt="Info" title="The number of tasks that should be concurrently scheduled for this port."></div>
            </div>
            <div class="setting-field">
                <span id="read-only-port-concurrent-tasks"></span>
            </div>
        </div>
        <div class="port-setting">
            <div class="setting-name">Comments</div>
            <div class="setting-field">
                <div id="read-only-port-comments"></div>
            </div>
        </div>
    </div>
</div>