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
package org.apache.nifi.processors.oozie.util;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;

import org.apache.hadoop.security.authentication.client.Authenticator;
import org.apache.hadoop.security.authentication.client.ConnectionConfigurator;
import org.apache.oozie.client.AuthOozieClient;
import org.apache.oozie.client.OozieClientException;

public class SslAuthOozieClient extends AuthOozieClient {

    private SSLContext sslContext = null;

    public SslAuthOozieClient(String oozieUrl) {
        super(oozieUrl);
    }

    public SslAuthOozieClient(String oozieUrl, String authOption) {
        super(oozieUrl, authOption);
    }

    public SslAuthOozieClient(String oozieUrl, String authOption, SSLContext context) {
        super(oozieUrl, authOption);
        this.sslContext = context;
    }

    @Override
    protected HttpURLConnection createConnection(URL url, String method) throws IOException, OozieClientException {
        if(sslContext == null) {
            return super.createConnection(url, method);
        } else {
            HttpsURLConnection conn = (HttpsURLConnection) super.createConnection(url, method);
            conn.setSSLSocketFactory(sslContext.getSocketFactory());
            return conn;
        }
    }

    @Override
    protected Authenticator getAuthenticator() throws OozieClientException {
        Authenticator auth = super.getAuthenticator();
        auth.setConnectionConfigurator(new SslConnectionConfigurator());
        return auth;
    }

    private class SslConnectionConfigurator implements ConnectionConfigurator {

        @Override
        public HttpURLConnection configure(HttpURLConnection conn) throws IOException {
            HttpsURLConnection sslConn = (HttpsURLConnection) conn;
            sslConn.setSSLSocketFactory(sslContext.getSocketFactory());
            return sslConn;
        }

    }

}
