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

import static java.lang.String.format;
import static java.lang.String.valueOf;

import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.http.HttpRequestor;
import com.dropbox.core.http.OkHttp3Requestor;
import com.dropbox.core.oauth.DbxCredential;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.FileMetadata;
import java.net.Proxy;
import java.util.HashMap;
import java.util.Map;
import okhttp3.Credentials;
import okhttp3.OkHttpClient;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dropbox.credentials.service.DropboxCredentialDetails;
import org.apache.nifi.dropbox.credentials.service.DropboxCredentialService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.proxy.ProxyConfiguration;

public interface DropboxTrait {

    String DROPBOX_HOME_URL = "https://www.dropbox.com/home";

    PropertyDescriptor CREDENTIAL_SERVICE = new PropertyDescriptor.Builder()
            .name("dropbox-credential-service")
            .displayName("Dropbox Credential Service")
            .description("Controller Service used to obtain Dropbox credentials (App Key, App Secret, Access Token, Refresh Token)." +
                    " See controller service's Additional Details for more information.")
            .identifiesControllerService(DropboxCredentialService.class)
            .required(true)
            .build();

    default DbxClientV2 getDropboxApiClient(ProcessContext context, String identifier) {
        final ProxyConfiguration proxyConfiguration = ProxyConfiguration.getConfiguration(context);
        final String dropboxClientId = format("%s-%s", getClass().getSimpleName(), identifier);
        final OkHttpClient.Builder okHttpClientBuilder = OkHttp3Requestor.defaultOkHttpClientBuilder();

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

        final HttpRequestor httpRequestor = new OkHttp3Requestor(okHttpClientBuilder.build());
        final DbxRequestConfig config = DbxRequestConfig.newBuilder(dropboxClientId)
                .withHttpRequestor(httpRequestor)
                .build();

        final DropboxCredentialService credentialService = context.getProperty(CREDENTIAL_SERVICE)
                .asControllerService(DropboxCredentialService.class);
        final DropboxCredentialDetails credential = credentialService.getDropboxCredential();

        return new DbxClientV2(config, new DbxCredential(credential.getAccessToken(), -1L,
                credential.getRefreshToken(), credential.getAppKey(), credential.getAppSecret()));
    }

    default String convertFolderName(String folderName) {
        return "/".equals(folderName) ? "" : folderName;
    }

    default String getParentPath(String fullPath) {
        final int idx = fullPath.lastIndexOf("/");
        final String parentPath = fullPath.substring(0, idx);
        return "".equals(parentPath) ? "/" : parentPath;
    }

    default Map<String, String> createAttributeMap(FileMetadata fileMetadata) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(DropboxAttributes.ID, fileMetadata.getId());
        attributes.put(DropboxAttributes.PATH, getParentPath(fileMetadata.getPathDisplay()));
        attributes.put(DropboxAttributes.FILENAME, fileMetadata.getName());
        attributes.put(DropboxAttributes.SIZE, valueOf(fileMetadata.getSize()));
        attributes.put(DropboxAttributes.REVISION, fileMetadata.getRev());
        attributes.put(DropboxAttributes.TIMESTAMP, valueOf(fileMetadata.getServerModified().getTime()));
        return attributes;
    }
}
