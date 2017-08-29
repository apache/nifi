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
package org.apache.nifi.ssl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.components.AllowableValue;

/**
 * Simple extension of the regular {@link SSLContextService} to allow for restricted implementations
 * of that interface.
 */
public interface RestrictedSSLContextService extends SSLContextService {

    /**
     * Build a restricted set of allowable TLS protocol algorithms.
     *
     * @return the computed set of allowable values
     */
    static AllowableValue[] buildAlgorithmAllowableValues() {
        final Set<String> supportedProtocols = new HashSet<>();

        /*
         * Prepopulate protocols with generic instance types commonly used
         * see: http://docs.oracle.com/javase/7/docs/technotes/guides/security/StandardNames.html#SSLContext
         */
        supportedProtocols.add("TLS");

        /*
         * Add specifically supported TLS versions
         */
        supportedProtocols.add("TLSv1.2");

        final int numProtocols = supportedProtocols.size();

        // Sort for consistent presentation in configuration views
        final List<String> supportedProtocolList = new ArrayList<>(supportedProtocols);
        Collections.sort(supportedProtocolList);

        final List<AllowableValue> protocolAllowableValues = new ArrayList<>();
        for (final String protocol : supportedProtocolList) {
            protocolAllowableValues.add(new AllowableValue(protocol));
        }
        return protocolAllowableValues.toArray(new AllowableValue[numProtocols]);
    }
}
