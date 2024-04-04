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
package org.apache.nifi.gcp.credentials.service;

import com.google.auth.oauth2.GoogleCredentials;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.exception.ProcessException;

/**
 * GCPCredentialsService interface to support getting Google Cloud Platform
 * GoogleCredentials used for instantiating Google cloud services.
 *
 * @see <a href="http://google.github.io/google-auth-library-java/releases/0.5.0/apidocs/com/google/auth/oauth2/GoogleCredentials.html">GoogleCredentials</a>
 */
@Tags({"gcp", "security", "credentials", "auth", "session"})
@CapabilityDescription("Provides GCP GoogleCredentials.")
public interface GCPCredentialsService extends ControllerService {

    /**
     * Get Google Credentials
     * @return Valid Google Credentials suitable for authorizing requests on the platform.
     * @throws ProcessException process exception in case there is problem in getting credentials
     */
    public GoogleCredentials getGoogleCredentials() throws ProcessException;
}
