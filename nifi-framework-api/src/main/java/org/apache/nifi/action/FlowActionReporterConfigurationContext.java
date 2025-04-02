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
package org.apache.nifi.action;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;
import java.util.Map;

/**
 * A context that will be passed to the reporter in order to obtain configuration.
 */
public interface FlowActionReporterConfigurationContext {
    /**
     * Retrieves all properties the context currently understands regardless
     * of whether a value has been set for them or not. If no value is present
     * then its value is null and thus any registered default for the property
     * descriptor applies.
     *
     * @return Map of all properties
     */
    Map<String, String> getProperties();

    /**
     * Retrieves SSLContext if set
     * @return SSLContext
     */
    SSLContext getSSLContext();

    /**
     * Retrieves the trust manager if set
     * @return X509TrustManager
     */
    X509TrustManager getTrustManager();
}
