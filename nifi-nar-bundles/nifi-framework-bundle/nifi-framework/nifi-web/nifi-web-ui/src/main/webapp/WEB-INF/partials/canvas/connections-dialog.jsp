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
<div id="connections-dialog" class="hidden">
    <div class="dialog-content">
        <div class="setting">
            <div class="setting-name">Selected component</div>
            <div class="setting-field">
                <div id="connections-context"></div>
            </div>
        </div>
        <div class="setting">
            <div id="connections-source" class="setting-name">Source</div>
            <div id="connections-destination" class="setting-name">Destination</div>
            <div class="clear"></div>
            <div class="setting-field">
                <div id="connections-container"></div>
            </div>
        </div>
    </div>
</div>