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
package org.apache.nifi.processors.standard.util;

import java.io.IOException;
import java.net.Proxy;
import java.util.HashMap;
import java.util.Map;

import com.burgstaller.okhttp.DispatchingAuthenticator;
import com.squareup.okhttp.Authenticator;
import com.squareup.okhttp.Credentials;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;

public class MultiAuthenticator extends DispatchingAuthenticator {

    public MultiAuthenticator(Map<String, Authenticator> registry) {
        super(registry);
    }

    private String proxyUsername;
    private String proxyPassword;

    @Override
    public Request authenticateProxy(Proxy proxy, Response response) throws IOException {
        String credential = Credentials.basic(proxyUsername, proxyPassword);
        return response.request()
                .newBuilder()
                .header("Proxy-Authorization", credential)
                .build();
    }

    public void setProxyUsername(String proxyUsername) {
        this.proxyUsername = proxyUsername;
    }

    public void setProxyPassword(String proxyPassword) {
        this.proxyPassword = proxyPassword;
    }

    public static final class Builder {
        Map<String, Authenticator> registry = new HashMap<>();

        public Builder with(String scheme, Authenticator authenticator) {
            registry.put(scheme, authenticator);
            return this;
        }

        public MultiAuthenticator build() {
            return new MultiAuthenticator(registry);
        }
    }

}
