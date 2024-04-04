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
package org.apache.nifi.processors.salesforce.util;

import org.apache.nifi.oauth2.StandardOauth2AccessTokenProvider;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;

/**
 * Set the following constants:<p>
 * VERSION<p>
 * BASE_URL<p>
 * USERNAME<p>
 * PASSWORD<p>
 * CLIENT_ID<p>
 * CLIENT_SECRET<p>
 */
public interface SalesforceConfigAware {
    String VERSION = "54.0";
    String INSTANCE_URL = "https://MyDomainName.my.salesforce.com";

    String AUTHORIZATION_SERVER_URL = INSTANCE_URL + "/services/oauth2/token";
    String USERNAME = "???";
    String PASSWORD = "???";
    String CLIENT_ID = "???";
    String CLIENT_SECRET = "???";

    default StandardOauth2AccessTokenProvider initOAuth2AccessTokenProvider(TestRunner runner) throws InitializationException {
        StandardOauth2AccessTokenProvider oauth2AccessTokenProvider = new StandardOauth2AccessTokenProvider();

        runner.addControllerService("oauth2AccessTokenProvider", oauth2AccessTokenProvider);

        runner.setProperty(oauth2AccessTokenProvider, StandardOauth2AccessTokenProvider.AUTHORIZATION_SERVER_URL, AUTHORIZATION_SERVER_URL);
        runner.setProperty(oauth2AccessTokenProvider, StandardOauth2AccessTokenProvider.USERNAME, USERNAME);
        runner.setProperty(oauth2AccessTokenProvider, StandardOauth2AccessTokenProvider.PASSWORD, PASSWORD);
        runner.setProperty(oauth2AccessTokenProvider, StandardOauth2AccessTokenProvider.CLIENT_ID, CLIENT_ID);
        runner.setProperty(oauth2AccessTokenProvider, StandardOauth2AccessTokenProvider.CLIENT_SECRET, CLIENT_SECRET);

        runner.enableControllerService(oauth2AccessTokenProvider);

        return oauth2AccessTokenProvider;
    }
}
