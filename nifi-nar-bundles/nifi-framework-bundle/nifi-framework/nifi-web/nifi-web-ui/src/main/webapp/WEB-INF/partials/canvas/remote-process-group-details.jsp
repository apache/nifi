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
<div id="remote-process-group-details" nf-draggable="{containment: 'parent', handle: '.dialog-header'}">
    <div class="dialog-content">
        <div class="setting">
            <div class="setting-name">Name</div>
            <div class="setting-field">
                <span id="read-only-remote-process-group-name"></span>
            </div>
        </div>
        <div class="setting">
            <div class="setting-name">Id</div>
            <div class="setting-field">
                <span id="read-only-remote-process-group-id"></span>
            </div>
        </div>
        <div class="setting">
            <div class="setting-name">URL</div>
            <div class="setting-field">
                <span id="read-only-remote-process-group-url"></span>
            </div>
        </div>
        <div class="setting">
            <div class="setting-name">Communications timeout</div>
            <div class="setting-field">
                <span id="read-only-remote-process-group-timeout"></span>
            </div>
        </div>
        <div class="setting">
            <div class="setting-name">
                Yield duration
                <img class="setting-icon icon-info" src="images/iconInfo.png" alt="Info" title="When communication with this remote process group fails, it will not be scheduled again until this amount of time elapses."/>
            </div>
            <div class="setting-field">
                <span id="read-only-remote-process-group-yield-duration"></span>
            </div>
        </div>
    </div>
</div>