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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertTrue;

import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.net.ssl.SSLContext;

import org.apache.nifi.components.AllowableValue;
import org.junit.Test;

public class SSLContextServiceUtilsTest {

    @Test
    public void testBuildRestrictedSSLProtocolAllowableValues() {
        final AllowableValue[] allowableValues = SSLContextServiceUtils.buildSSLAlgorithmAllowableValues(true);
        assertThat(allowableValues, notNullValue());
        assertThat(allowableValues.length, equalTo(1));
        assertThat(allowableValues[0].getValue(), equalTo("TLSv1.2"));
    }

    @Test
    public void testBuildUnrestrictedSSLProtocolAllowableValues() throws NoSuchAlgorithmException {
        final AllowableValue[] allowableValues = SSLContextServiceUtils.buildSSLAlgorithmAllowableValues(false);

        // we expect TLS, SSL, and all available configured JVM protocols
        final Set<String> expected = new HashSet<>();
        expected.add("SSL");
        expected.add("TLS");
        final String[] supportedProtocols = SSLContext.getDefault().createSSLEngine().getSupportedProtocols();
        expected.addAll(Arrays.asList(supportedProtocols));

        assertThat(allowableValues, notNullValue());
        assertThat(allowableValues.length, equalTo(expected.size()));
        for(final AllowableValue value : allowableValues) {
            assertTrue(expected.contains(value.getValue()));
        }
    }
}
