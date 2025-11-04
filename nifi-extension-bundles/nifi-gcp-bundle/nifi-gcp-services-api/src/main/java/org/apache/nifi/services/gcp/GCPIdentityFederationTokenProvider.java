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
package org.apache.nifi.services.gcp;

import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.GoogleCredentials;
import org.apache.nifi.controller.ControllerService;

/**
 * Provides Google Cloud credentials obtained through Workload Identity Federation.
 * Implementations are expected to return credentials capable of refreshing themselves
 * using the configured token exchange mechanism.
 */
public interface GCPIdentityFederationTokenProvider extends ControllerService {

    /**
     * Create Google Cloud credentials backed by workload identity federation.
     *
     * @param transportFactory Transport factory to use when contacting Google Security Token Service
     * @return Google credentials that can obtain and refresh Google Cloud access tokens
     */
    GoogleCredentials getGoogleCredentials(HttpTransportFactory transportFactory);
}
