/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.web.security.oidc;

public interface OIDCEndpoints {

    String OIDC_ACCESS_ROOT = "/access/oidc";

    String LOGIN_REQUEST_RELATIVE = "/request";
    String LOGIN_REQUEST = OIDC_ACCESS_ROOT + LOGIN_REQUEST_RELATIVE;

    String LOGIN_CALLBACK_RELATIVE = "/callback";
    String LOGIN_CALLBACK = OIDC_ACCESS_ROOT + LOGIN_CALLBACK_RELATIVE;

    String TOKEN_EXCHANGE_RELATIVE = "/exchange";
    String TOKEN_EXCHANGE = OIDC_ACCESS_ROOT + TOKEN_EXCHANGE_RELATIVE;

    String LOGOUT_REQUEST_RELATIVE = "/logout";
    String LOGOUT_REQUEST = OIDC_ACCESS_ROOT + LOGOUT_REQUEST_RELATIVE;

    String LOGOUT_CALLBACK_RELATIVE = "/logoutCallback";
    String LOGOUT_CALLBACK = OIDC_ACCESS_ROOT + LOGOUT_CALLBACK_RELATIVE;
}
