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
import com.dropbox.core.http.StandardHttpRequestor;
import com.dropbox.core.oauth.DbxCredential;
import com.dropbox.core.v2.DbxClientV2;
import java.net.Proxy;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dropbox.credentials.service.DropboxCredentialDetails;
import org.apache.nifi.dropbox.credentials.service.DropboxCredentialService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.proxy.ProxyConfiguration;

public interface DropboxTrait {

    PropertyDescriptor CREDENTIAL_SERVICE = new PropertyDescriptor.Builder()
            .name("dropbox-credential-service")
            .displayName("Dropbox Credential Service")
            .description("Controller Service used to obtain Dropbox credentials." +
                    " See controller service's usage documentation for more details.")
            .identifiesControllerService(DropboxCredentialService.class)
            .required(true)
            .build();

    default DbxClientV2 getDropboxApiClient(ProcessContext context, ProxyConfiguration proxyConfiguration, String clientId) {
        Proxy proxy = proxyConfiguration.createProxy();
        StandardHttpRequestor.Config requesterConfig = StandardHttpRequestor.Config.builder()
                .withProxy(proxy)
                .build();
        StandardHttpRequestor httpRequester = new StandardHttpRequestor(requesterConfig);
        DbxRequestConfig config = DbxRequestConfig.newBuilder(clientId)
                .withHttpRequestor(httpRequester)
                .build();

        final DropboxCredentialService credentialService = context.getProperty(CREDENTIAL_SERVICE)
                .asControllerService(DropboxCredentialService.class);
        DropboxCredentialDetails credential = credentialService.getDropboxCredential();

        return new DbxClientV2(config, new DbxCredential(credential.getAccessToken(), -1L,
                credential.getRefreshToken(), credential.getAppKey(), credential.getAppSecret()));
    }
}
