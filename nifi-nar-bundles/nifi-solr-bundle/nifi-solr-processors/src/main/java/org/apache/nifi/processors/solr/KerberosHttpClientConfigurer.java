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
package org.apache.nifi.processors.solr;

import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthSchemeRegistry;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.params.ClientPNames;
import org.apache.http.entity.BufferedHttpEntity;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.HttpContext;
import org.apache.solr.client.solrj.impl.HttpClientConfigurer;
import org.apache.solr.client.solrj.impl.SolrPortAwareCookieSpecFactory;
import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.security.Principal;

/**
 * This class is a modified version of Krb5HttpClientConfigurer that is part of SolrJ.
 *
 * In our case we don't want to warn about the useSubjectCreds property since we know we are going to do a
 * login and will have subject creds, and we also don't want to mess the static JAAS configuration of the JVM.
 */
public class KerberosHttpClientConfigurer extends HttpClientConfigurer {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public void configure(DefaultHttpClient httpClient, SolrParams config) {
        super.configure(httpClient, config);
        logger.info("Setting up SPNego auth...");

        //Enable only SPNEGO authentication scheme.
        final AuthSchemeRegistry registry = new AuthSchemeRegistry();
        registry.register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory(true, false));
        httpClient.setAuthSchemes(registry);

        // Get the credentials from the JAAS configuration rather than here
        final Credentials useJaasCreds = new Credentials() {
            public String getPassword() {
                return null;
            }
            public Principal getUserPrincipal() {
                return null;
            }
        };

        final SolrPortAwareCookieSpecFactory cookieFactory = new SolrPortAwareCookieSpecFactory();
        httpClient.getCookieSpecs().register(cookieFactory.POLICY_NAME, cookieFactory);
        httpClient.getParams().setParameter(ClientPNames.COOKIE_POLICY, cookieFactory.POLICY_NAME);
        httpClient.getCredentialsProvider().setCredentials(AuthScope.ANY, useJaasCreds);
        httpClient.addRequestInterceptor(bufferedEntityInterceptor);
    }

    // Set a buffered entity based request interceptor
    private HttpRequestInterceptor bufferedEntityInterceptor = new HttpRequestInterceptor() {
        @Override
        public void process(HttpRequest request, HttpContext context) throws HttpException,
                IOException {
            if(request instanceof HttpEntityEnclosingRequest) {
                HttpEntityEnclosingRequest enclosingRequest = ((HttpEntityEnclosingRequest) request);
                HttpEntity requestEntity = enclosingRequest.getEntity();
                enclosingRequest.setEntity(new BufferedHttpEntity(requestEntity));
            }
        }
    };

}
