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
<%@ taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt" %>
<div id="nf-about">
    <div id="nf-about-pic-container">
        <div id="nf-about-pic"></div>
    </div>
    <div class="dialog-content">
        <div id="nf-about-content">
            <span id="nf-version"></span>
            <div id="nf-version-detail">
                <p id="nf-version-detail-timestamp">
                    <span id="nf-about-build-timestamp"></span>
                </p>
                <p id="nf-version-detail-tag">
                    <fmt:message key="partials.canvas.about-dailog.Tagged"/> <span id="nf-about-build-tag"></span>
                </p>
                <p id="nf-version-detail-commit">
                    <fmt:message key="partials.canvas.about-dailog.From"/> <span id="nf-about-build-revision"></span> <fmt:message key="partials.canvas.about-dailog.OnBranch"/> <span id="nf-about-build-branch"></span>
                </p>
            </div>
            <p>
                <fmt:message key="partials.canvas.about-dailog.nf-version"/>
            </p>
        </div>
    </div>
</div>
