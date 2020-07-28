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
<div id="new-process-group-dialog" class="hidden small-dialog">
    <div class="dialog-content">
        <div class="setting">
            <div class="setting-name">Process group name</div>
            <div class="setting-field">
                <input id="new-process-group-name" type="text"/>
            </div>
        </div>
        <div class="setting">
            <span id="import-process-group-link" class="link" title="Import a flow from a registry">
                <i class="fa fa-cloud-download" aria-hidden="true" style="margin-left: 5px; margin-right: 5px;"></i>
                Import...
            </span>
        </div>
    </div>
</div>