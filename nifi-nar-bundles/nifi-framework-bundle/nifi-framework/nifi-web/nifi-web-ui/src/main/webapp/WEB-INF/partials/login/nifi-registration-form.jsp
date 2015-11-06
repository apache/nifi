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
<div id="nifi-registration-container" class="hidden">
    <div id="nifi-registration-title" class="login-title nifi-submit-justification">Submit Justification</div>
    <div id="nifi-user-submit-justification-container" class="nifi-submit-justification">
        <div class="setting">
            <div class="setting-name">User</div>
            <div class="setting-field">
                <div id="nifi-user-submit-justification"></div>
            </div>
        </div>
    </div>
    <div class="setting">
        <div class="setting-name">Justification</div>
        <div class="setting-field">
            <textarea cols="30" rows="4" id="nifi-registration-justification" maxlength="500" class="setting-input"></textarea>
        </div>
        <div id="login-to-account-message" class="hidden">
            <div style="font-style: italic;">Already have an account?</div>
            <div style="margin-top: 2px;"><span id="login-to-account-link" class="link">Log in</span></div>
        </div>
        <div style="text-align: right; color: #666; margin-top: 2px; float: right;">
            <span id="remaining-characters"></span>&nbsp;characters remaining
        </div>
        <div class="clear"></div>
    </div>
</div>