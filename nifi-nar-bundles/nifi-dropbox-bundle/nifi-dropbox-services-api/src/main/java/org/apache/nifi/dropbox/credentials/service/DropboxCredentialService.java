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
package org.apache.nifi.dropbox.credentials.service;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;

/**
 * DropboxCredentialService interface to support getting Dropbox credentials (app key, app secret, access token, refresh token)
 *
 *
 * @see <a href="https://www.dropbox.com/developers/reference/getting-started">Dropbox Developers Getting Started</a>
 * @see <a href="https://dropbox.github.io/dropbox-sdk-java/api-docs/v4.0.0/com/dropbox/core/oauth/DbxCredential.html">DbxCredential</a>
 */
@Tags({"dropbox", "credentials", "auth", "session"})
@CapabilityDescription("Provides DropboxCredentialDetails.")
public interface DropboxCredentialService extends ControllerService {
    /**
     * Get Dropbox credentials
     * @return  DropboxCredentialDetails object which contains app key, app secret, access and refresh tokens for authorizing
     * requests on the Dropbox platform.
     */
    DropboxCredentialDetails getDropboxCredential();
}
