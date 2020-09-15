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
package org.apache.nifi.web.security.saml;

public interface SAMLEndpoints {

    String SERVICE_PROVIDER_METADATA_RELATIVE = "/saml/metadata";
    String SERVICE_PROVIDER_METADATA = "/access" + SERVICE_PROVIDER_METADATA_RELATIVE;

    String LOGIN_REQUEST_RELATIVE = "/saml/login/request";
    String LOGIN_REQUEST = "/access" + LOGIN_REQUEST_RELATIVE;

    String LOGIN_CONSUMER_RELATIVE = "/saml/login/consumer";
    String LOGIN_CONSUMER = "/access" + LOGIN_CONSUMER_RELATIVE;

    String LOGIN_EXCHANGE_RELATIVE = "/saml/login/exchange";
    String LOGIN_EXCHANGE = "/access" + LOGIN_EXCHANGE_RELATIVE;

    String LOCAL_LOGOUT_RELATIVE = "/saml/local-logout";
    String LOCAL_LOGOUT = "/access" + LOCAL_LOGOUT_RELATIVE;

    String SINGLE_LOGOUT_REQUEST_RELATIVE = "/saml/single-logout/request";
    String SINGLE_LOGOUT_REQUEST = "/access" + SINGLE_LOGOUT_REQUEST_RELATIVE;

    String SINGLE_LOGOUT_CONSUMER_RELATIVE = "/saml/single-logout/consumer";
    String SINGLE_LOGOUT_CONSUMER = "/access" + SINGLE_LOGOUT_CONSUMER_RELATIVE;

}
