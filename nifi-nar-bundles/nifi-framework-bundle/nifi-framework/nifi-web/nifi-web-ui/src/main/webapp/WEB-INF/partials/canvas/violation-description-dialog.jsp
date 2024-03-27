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
<div id="violation-menu-more-info-dialog" layout="column" class="hidden medium-dialog">
    <div class="dialog-content">
        <div class="violation-info-head">
            <div id="violation-name" class="violation-name">Violation</div>
            <div id="violation-type-pill" class="violation-type-pill"></div>
        </div>
        <div id="violation-display-name" class="violation-display-name"></div>

        <p id="violation-description" class="violation-description"></p>

        <i class="fa fa-book violation-docs-link-icon" aria-hidden="true"></i><a href="" class="violation-docs-link">View Documentation</a>
    </div>
</div>