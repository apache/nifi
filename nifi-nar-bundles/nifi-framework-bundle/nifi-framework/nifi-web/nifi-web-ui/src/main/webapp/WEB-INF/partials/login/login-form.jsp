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
<div id="login-container" class="hidden">
    <div class="login-title"><fmt:message key="paritals.login.login-form.login-title"/></div>
    <div class="setting">
        <div class="setting-name"><fmt:message key="paritals.login.login-form.Username"/></div>
        <div class="setting-field">
        	<fmt:message key="paritals.login.login-form.User" var="user"/>
            <input type="text" placeholder="${user}" id="username"/>
        </div>
    </div>
    <div class="setting">
        <div class="setting-name"><fmt:message key="paritals.login.login-form.Password"/></div>
        <div class="setting-field">
        	<fmt:message key="paritals.login.login-form.Password" var="password"/>
            <input type="password" placeholder="${password}" id="password"/>
        </div>
    </div>
</div>