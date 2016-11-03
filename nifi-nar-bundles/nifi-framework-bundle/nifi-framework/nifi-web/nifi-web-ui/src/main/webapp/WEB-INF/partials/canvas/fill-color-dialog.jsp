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
<div id="fill-color-dialog" class="hidden">
    <div class="dialog-content">
        <div class="setting" style="margin-bottom: 0px;">
            <div class="setting-name">Color</div>
            <div class="setting-field">
                <input type="text" id="fill-color" value="#FFFFFF"/>
            </div>
            <div class="setting-name" style="margin-top: 10px;">Value</div>
            <div class="setting-field">
                <input type="text" id="fill-color-value" value="#FFFFFF"/>
            </div>
            <div class="setting-name" style="margin-top: 10px;">Preview</div>
            <div class="setting-field">
                <div id="fill-color-processor-preview">
                    <div id="fill-color-processor-preview-icon" class="icon icon-processor"></div>
                    <div id="fill-color-processor-preview-name" style="margin-left: 35px; line-height: 25px; font-size: 12px; height: 25px; color: #262626;">Processor Name</div>
                    <div style="width: 100%; height: 9px; border-bottom: 1px solid #c7d2d7; background-color: #f4f6f7;"></div>
                    <div style="width: 100%; height: 9px; border-bottom: 1px solid #c7d2d7; background-color: #ffffff;"></div>
                    <div style="width: 100%; height: 10px; border-bottom: 1px solid #c7d2d7; background-color: #f4f6f7;"></div>
                    <div style="width: 100%; height: 9px; background-color: #ffffff;"></div>
                </div>
                <div id="fill-color-label-preview">
                    <div id="fill-color-label-preview-value">Label Value</div>
                </div>
            </div>
        </div>
    </div>
</div>