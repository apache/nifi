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

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.net.ssl.SSLContext;

import org.apache.nifi.components.AllowableValue;

public class SSLContextServiceUtils {

    /**
     * Build a set of allowable SSL protocol algorithms based on JVM configuration and whether
     * or not the list should be restricted to include only certain protocols.
     *
     * @param restricted whether the set of allowable protocol values should be restricted.
     *
     * @return the computed set of allowable values
     */
    public static AllowableValue[] buildSSLAlgorithmAllowableValues(final boolean restricted) {
        final Set<String> supportedProtocols = new HashSet<>();

        // if restricted, only allow the below set of protocols.
        if(restricted) {
            supportedProtocols.add("TLSv1.2");

        } else {

            /*
             * Prepopulate protocols with generic instance types commonly used
             * see: http://docs.oracle.com/javase/7/docs/technotes/guides/security/StandardNames.html#SSLContext
             */
            supportedProtocols.add("SSL");
            supportedProtocols.add("TLS");

            // Determine those provided by the JVM on the system
            try {
                supportedProtocols.addAll(Arrays.asList(SSLContext.getDefault().createSSLEngine().getSupportedProtocols()));
            } catch (NoSuchAlgorithmException e) {
                // ignored as default is used
            }
        }

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
