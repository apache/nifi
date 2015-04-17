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
<div id="registration-pane" class="message-pane hidden">
    <div class="message-pane-message-box">
        <p id="register-title" class="message-pane-title">You are not authorized to access this data flow</p>
        <p id="register-content" class="message-pane-content">
        <div>
            <div id="expand-registration-button" class="collapsed pointer"></div>
            <span id="expand-registration-text" class="link">Request Access</span>
        </div>
        <div id="registration-form" class="settings hidden">
            <div class="setting">
                <div class="setting-name">Justification</div>
                <div class="setting-field">
                    <textarea cols="30" rows="4" id="registration-justification" maxlength="500" name="registration-justification" class="setting-input"></textarea>
                </div>
                <div style="text-align: right; color: #666; margin-top: 2px;">
                    <span id="remaining-characters"></span>&nbsp;characters remaining
                </div>
                <div class="clear"></div>
            </div>
            <div>
                <div id="registration-form-submit" class="button">Submit</div>
                <div class="clear"></div>
            </div>
        </div>
        </p>
    </div>
</div>