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
<div id="parameter-contexts" class="hidden">
    <button id="new-parameter-context" class="add-button fa fa-plus" title="Create a new parameter context" style="display: block;"></button>
    <div id="parameter-contexts-header-text" class="parameter-contexts-header-text">NiFi Parameter Contexts</div>
    <div class="parameter-contexts-container">
        <div id="parameter-contexts-table" class="parameter-contexts-table"></div>
    </div>
    <div id="parameter-contexts-refresh-container">
        <button id="parameter-contexts-refresh-button" class="refresh-button pointer fa fa-refresh" title="Refresh"></button>
        <div id="parameter-contexts-last-refreshed-container" class="last-refreshed-container">
            Last updated:&nbsp;<span id="parameter-contexts-last-refreshed" class="value-color"></span>
        </div>
        <div id="parameter-contexts-loading-container" class="loading-container"></div>
        <div class="clear"></div>
    </div>
</div>
