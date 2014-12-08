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
<div id="group-roles-dialog">
    <div class="dialog-content">
        <div class="setting">
            <div class="setting-name">Group</div>
            <div class="setting-field">
                <span id="group-name-roles-dialog"></span>
            </div>
            <div class="clear"></div>
        </div>
        <div class="setting">
            <div class="setting-name">Roles</div>
            <div class="group-roles-container">
                <div class="role-container">
                    <div id="group-role-admin-checkbox" class="role-checkbox nf-checkbox checkbox-unchecked"></div><div class="role-name">Administrator</div>
                </div>
                <div class="role-container">
                    <div id="group-role-dfm-checkbox" class="role-checkbox nf-checkbox checkbox-unchecked"></div><div class="role-name">Data Flow Manager</div>
                </div>
                <div class="role-container">
                    <div id="group-role-monitor-checkbox" class="role-checkbox nf-checkbox checkbox-unchecked"></div><div class="role-name">Read Only</div>
                </div>
                <div class="role-container" style="margin-top: 5px;">
                    <div id="group-role-provenance-checkbox" class="role-checkbox nf-checkbox checkbox-unchecked"></div><div class="role-name">Provenance</div>
                </div>
                <div class="role-container" style="margin-top: 5px;">
                    <div id="group-role-nifi-checkbox" class="role-checkbox nf-checkbox checkbox-unchecked"></div><div class="role-name">NiFi</div>
                </div>
                <div class="role-container">
                    <div id="group-role-proxy-checkbox" class="role-checkbox nf-checkbox checkbox-unchecked"></div><div class="role-name">Proxy</div>
                </div>
            </div>
            <div class="clear"></div>
        </div>
    </div>
</div>
