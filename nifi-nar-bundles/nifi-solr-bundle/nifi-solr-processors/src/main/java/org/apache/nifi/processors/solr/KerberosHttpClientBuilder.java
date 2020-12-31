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

import java.lang.invoke.MethodHandles;
import java.security.Principal;
import java.util.Optional;

import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.config.Lookup;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.cookie.CookieSpecProvider;
import org.apache.http.entity.BufferedHttpEntity;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.solr.client.solrj.impl.HttpClientBuilderFactory;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.SolrHttpClientBuilder;
import org.apache.solr.client.solrj.impl.SolrPortAwareCookieSpecFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is a modified version of Krb5HttpClientBuilder that is part of SolrJ.
 *
 * In our case we don't want to warn about the useSubjectCreds property since we know we are going to do a
 * login and will have subject creds, and we also don't want to mess the static JAAS configuration of the JVM.
 */
public class KerberosHttpClientBuilder implements HttpClientBuilderFactory {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public KerberosHttpClientBuilder() {

    }

    public SolrHttpClientBuilder getBuilder() {
        return getBuilder(HttpClientUtil.getHttpClientBuilder());
    }

    public void close() {
        HttpClientUtil.removeRequestInterceptor(bufferedEntityInterceptor);
    }

    @Override
    public SolrHttpClientBuilder getHttpClientBuilder(Optional<SolrHttpClientBuilder> builder) {
        return builder.isPresent() ? getBuilder(builder.get()) : getBuilder();
    }

    public SolrHttpClientBuilder getBuilder(SolrHttpClientBuilder builder) {

        //Enable only SPNEGO authentication scheme.

        builder.setAuthSchemeRegistryProvider(() -> {
            Lookup<AuthSchemeProvider> authProviders = RegistryBuilder.<AuthSchemeProvider>create()
                    .register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory(true, false))
                    .build();
            return authProviders;
        });
        // Get the credentials from the JAAS configuration rather than here
        Credentials useJaasCreds = new Credentials() {
            public String getPassword() {
                return null;
            }
            public Principal getUserPrincipal() {
                return null;
            }
        };

        HttpClientUtil.setCookiePolicy(SolrPortAwareCookieSpecFactory.POLICY_NAME);

        builder.setCookieSpecRegistryProvider(() -> {
            SolrPortAwareCookieSpecFactory cookieFactory = new SolrPortAwareCookieSpecFactory();

            Lookup<CookieSpecProvider> cookieRegistry = RegistryBuilder.<CookieSpecProvider> create()
                    .register(SolrPortAwareCookieSpecFactory.POLICY_NAME, cookieFactory).build();

            return cookieRegistry;
        });

        builder.setDefaultCredentialsProvider(() -> {
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, useJaasCreds);
            return credentialsProvider;
        });
        HttpClientUtil.addRequestInterceptor(bufferedEntityInterceptor);
        return builder;
    }

    // Set a buffered entity based request interceptor
    private HttpRequestInterceptor bufferedEntityInterceptor = (request, context) -> {
        if(request instanceof HttpEntityEnclosingRequest) {
            HttpEntityEnclosingRequest enclosingRequest = ((HttpEntityEnclosingRequest) request);
            HttpEntity requestEntity = enclosingRequest.getEntity();
            enclosingRequest.setEntity(new BufferedHttpEntity(requestEntity));
        }
    };
}
