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
package org.apache.nifi.processors.dropbox;

import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.http.HttpRequestor;
import com.dropbox.core.http.OkHttp3Requestor;
import com.dropbox.core.oauth.DbxCredential;
import com.dropbox.core.v2.DbxClientV2;
import java.net.Proxy;
import okhttp3.Credentials;
import okhttp3.OkHttpClient;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dropbox.credentials.service.DropboxCredentialDetails;
import org.apache.nifi.dropbox.credentials.service.DropboxCredentialService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.proxy.ProxyConfiguration;

public interface DropboxTrait {

    PropertyDescriptor CREDENTIAL_SERVICE = new PropertyDescriptor.Builder()
            .name("dropbox-credential-service")
            .displayName("Dropbox Credential Service")
            .description("Controller Service used to obtain Dropbox credentials (App Key, App Secret, Access Token, Refresh Token)." +
                    " See controller service's Additional Details for more information.")
            .identifiesControllerService(DropboxCredentialService.class)
            .required(true)
            .build();


    default DbxClientV2 getDropboxApiClient(ProcessContext context, ProxyConfiguration proxyConfiguration, String clientId) {
        OkHttpClient.Builder okHttpClientBuilder = OkHttp3Requestor.defaultOkHttpClientBuilder();

        if (!Proxy.Type.DIRECT.equals(proxyConfiguration.getProxyType())) {
            okHttpClientBuilder.proxy(proxyConfiguration.createProxy());

            if (proxyConfiguration.hasCredential()) {
                okHttpClientBuilder.proxyAuthenticator((route, response) -> {
                    final String credential = Credentials.basic(proxyConfiguration.getProxyUserName(), proxyConfiguration.getProxyUserPassword());
                    return response.request().newBuilder()
                            .header("Proxy-Authorization", credential)
                            .build();
                });
            }
        }

        HttpRequestor httpRequestor = new OkHttp3Requestor(okHttpClientBuilder.build());
        DbxRequestConfig config = DbxRequestConfig.newBuilder(clientId)
                .withHttpRequestor(httpRequestor)
                .build();

        final DropboxCredentialService credentialService = context.getProperty(CREDENTIAL_SERVICE)
                .asControllerService(DropboxCredentialService.class);
        DropboxCredentialDetails credential = credentialService.getDropboxCredential();

        return new DbxClientV2(config, new DbxCredential(credential.getAccessToken(), -1L,
                credential.getRefreshToken(), credential.getAppKey(), credential.getAppSecret()));
    }
}
