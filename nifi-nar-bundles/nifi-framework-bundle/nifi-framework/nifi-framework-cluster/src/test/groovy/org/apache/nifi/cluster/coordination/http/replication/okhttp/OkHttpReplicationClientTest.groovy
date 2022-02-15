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


package org.apache.nifi.cluster.coordination.http.replication.okhttp

import org.apache.nifi.security.util.TemporaryKeyStoreBuilder
import org.apache.nifi.security.util.TlsConfiguration
import org.apache.nifi.util.NiFiProperties
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4.class)
class OkHttpReplicationClientTest extends GroovyTestCase {
    private static TlsConfiguration tlsConfiguration

    @BeforeClass
    static void setUpOnce() throws Exception {
        tlsConfiguration = new TemporaryKeyStoreBuilder().build()
    }

    private static NiFiProperties mockNiFiProperties() {
        return NiFiProperties.createBasicNiFiProperties(null)
    }

    @Test
    void testShouldReplaceNonZeroContentLengthHeader() {
        // Arrange
        def headers = ["Content-Length": "123", "Other-Header": "arbitrary value"]
        String method = "DELETE"

        NiFiProperties mockProperties = mockNiFiProperties()

        OkHttpReplicationClient client = new OkHttpReplicationClient(mockProperties)

        // Act
        client.checkContentLengthHeader(method, headers)

        // Assert
        assert headers.size() == 2
        assert headers."Content-Length" == "0"
    }

    @Test
    void testShouldReplaceNonZeroContentLengthHeaderOnDeleteCaseInsensitive() {
        // Arrange
        def headers = ["Content-Length": "123", "Other-Header": "arbitrary value"]
        String method = "delete"

        NiFiProperties mockProperties = mockNiFiProperties()

        OkHttpReplicationClient client = new OkHttpReplicationClient(mockProperties)

        // Act
        client.checkContentLengthHeader(method, headers)

        // Assert
        assert headers.size() == 2
        assert headers."Content-Length" == "0"
    }

    @Test
    void testShouldNotReplaceContentLengthHeaderWhenZeroOrNull() {
        // Arrange
        String method = "DELETE"
        def zeroOrNullContentLengths = [null, "0"]

        NiFiProperties mockProperties = mockNiFiProperties()

        OkHttpReplicationClient client = new OkHttpReplicationClient(mockProperties)

        // Act
        zeroOrNullContentLengths.each { String contentLength ->
            def headers = ["Content-Length": contentLength, "Other-Header": "arbitrary value"]
            client.checkContentLengthHeader(method, headers)

            // Assert
            assert headers.size() == 2
            assert headers."Content-Length" == contentLength
        }
    }

    @Test
    void testShouldNotReplaceNonZeroContentLengthHeaderOnOtherMethod() {
        // Arrange
        def headers = ["Content-Length": "123", "Other-Header": "arbitrary value"]

        NiFiProperties mockProperties = mockNiFiProperties()

        OkHttpReplicationClient client = new OkHttpReplicationClient(mockProperties)

        def nonDeleteMethods = ["POST", "PUT", "GET", "HEAD"]

        // Act
        nonDeleteMethods.each { String method ->
            client.checkContentLengthHeader(method, headers)

            // Assert
            assert headers.size() == 2
            assert headers."Content-Length" == "123"
        }
    }

    @Test
    void testShouldUseKeystorePasswordIfKeyPasswordIsBlank() {
        // Arrange
        Map propsMap = [
                (NiFiProperties.SECURITY_TRUSTSTORE)       : tlsConfiguration.truststorePath,
                (NiFiProperties.SECURITY_TRUSTSTORE_TYPE)  : tlsConfiguration.truststoreType.type,
                (NiFiProperties.SECURITY_TRUSTSTORE_PASSWD): tlsConfiguration.truststorePassword,
                (NiFiProperties.SECURITY_KEYSTORE)         : tlsConfiguration.keystorePath,
                (NiFiProperties.SECURITY_KEYSTORE_TYPE)    : tlsConfiguration.keystoreType.type,
                (NiFiProperties.SECURITY_KEYSTORE_PASSWD)  : tlsConfiguration.keystorePassword,
                (NiFiProperties.SECURITY_KEY_PASSWD)       : "",
                (NiFiProperties.WEB_HTTPS_HOST)            : "localhost",
                (NiFiProperties.WEB_HTTPS_PORT)            : "51552",
        ]
        NiFiProperties mockNiFiProperties = new NiFiProperties(new Properties(propsMap))

        // Act
        OkHttpReplicationClient client = new OkHttpReplicationClient(mockNiFiProperties)

        // Assert
        assert client.isTLSConfigured()
    }

    @Test
    void testShouldUseKeystorePasswordIfKeyPasswordIsNull() {
        // Arrange
        Map flowfileEncryptionProps = [
                (NiFiProperties.SECURITY_TRUSTSTORE)       : tlsConfiguration.truststorePath,
                (NiFiProperties.SECURITY_TRUSTSTORE_TYPE)  : tlsConfiguration.truststoreType.type,
                (NiFiProperties.SECURITY_TRUSTSTORE_PASSWD): tlsConfiguration.truststorePassword,
                (NiFiProperties.SECURITY_KEYSTORE)         : tlsConfiguration.keystorePath,
                (NiFiProperties.SECURITY_KEYSTORE_TYPE)    : tlsConfiguration.keystoreType.type,
                (NiFiProperties.SECURITY_KEYSTORE_PASSWD)  : tlsConfiguration.keystorePassword,
                (NiFiProperties.WEB_HTTPS_HOST)            : "localhost",
                (NiFiProperties.WEB_HTTPS_PORT)            : "51552",
        ]
        NiFiProperties mockNiFiProperties = new NiFiProperties(new Properties(flowfileEncryptionProps))

        // Act
        OkHttpReplicationClient client = new OkHttpReplicationClient(mockNiFiProperties)

        // Assert
        assert client.isTLSConfigured()
    }

    @Test
    void testShouldFailIfKeyPasswordIsSetButKeystorePasswordIsBlank() {
        // Arrange
        Map propsMap = [
                (NiFiProperties.SECURITY_TRUSTSTORE)       : tlsConfiguration.truststorePath,
                (NiFiProperties.SECURITY_TRUSTSTORE_TYPE)  : tlsConfiguration.truststoreType.type,
                (NiFiProperties.SECURITY_TRUSTSTORE_PASSWD): tlsConfiguration.truststorePassword,
                (NiFiProperties.SECURITY_KEYSTORE)         : tlsConfiguration.keystorePath,
                (NiFiProperties.SECURITY_KEYSTORE_TYPE)    : tlsConfiguration.keystoreType.type,
                (NiFiProperties.SECURITY_KEYSTORE_PASSWD)  : tlsConfiguration.keystorePassword,
                (NiFiProperties.SECURITY_KEYSTORE_PASSWD)  : "",
                (NiFiProperties.WEB_HTTPS_HOST)            : "localhost",
                (NiFiProperties.WEB_HTTPS_PORT)            : "51552",
        ]
        NiFiProperties mockNiFiProperties = new NiFiProperties(new Properties(propsMap))

        // Act
        OkHttpReplicationClient client = new OkHttpReplicationClient(mockNiFiProperties)

        // Assert
        assert !client.isTLSConfigured()
    }

    @Test
    void testShouldFailIfKeyPasswordAndKeystorePasswordAreBlank() {
        // Arrange
        Map propsMap = [
                (NiFiProperties.SECURITY_TRUSTSTORE)       : tlsConfiguration.truststorePath,
                (NiFiProperties.SECURITY_TRUSTSTORE_TYPE)  : tlsConfiguration.truststoreType.type,
                (NiFiProperties.SECURITY_TRUSTSTORE_PASSWD): tlsConfiguration.truststorePassword,
                (NiFiProperties.SECURITY_KEYSTORE)         : tlsConfiguration.keystorePath,
                (NiFiProperties.SECURITY_KEYSTORE_TYPE)    : tlsConfiguration.keystoreType.type,
                (NiFiProperties.SECURITY_KEYSTORE_PASSWD)  : "",
                (NiFiProperties.SECURITY_KEY_PASSWD)       : "",
                (NiFiProperties.WEB_HTTPS_HOST)            : "localhost",
                (NiFiProperties.WEB_HTTPS_PORT)            : "51552",
        ]
        NiFiProperties mockNiFiProperties = new NiFiProperties(new Properties(propsMap))

        // Act
        OkHttpReplicationClient client = new OkHttpReplicationClient(mockNiFiProperties)

        // Assert
        assert !client.isTLSConfigured()
    }

    @Test
    void testShouldDetermineIfTLSConfigured() {
        // Arrange
        Map propsMap = [(NiFiProperties.WEB_HTTPS_HOST): "localhost",
                        (NiFiProperties.WEB_HTTPS_PORT): "51552",]

        Map tlsPropsMap = [
                (NiFiProperties.SECURITY_TRUSTSTORE)       : tlsConfiguration.truststorePath,
                (NiFiProperties.SECURITY_TRUSTSTORE_TYPE)  : tlsConfiguration.truststoreType.type,
                (NiFiProperties.SECURITY_TRUSTSTORE_PASSWD): tlsConfiguration.truststorePassword,
                (NiFiProperties.SECURITY_KEYSTORE)         : tlsConfiguration.keystorePath,
                (NiFiProperties.SECURITY_KEYSTORE_TYPE)    : tlsConfiguration.keystoreType.type,
                (NiFiProperties.SECURITY_KEYSTORE_PASSWD)  : tlsConfiguration.keystorePassword
        ] + propsMap


        NiFiProperties mockNiFiProperties = new NiFiProperties(new Properties(propsMap))
        NiFiProperties mockTLSNiFiProperties = new NiFiProperties(new Properties(tlsPropsMap))

        // Remove the keystore password to create an invalid configuration
        Map invalidTlsPropsMap = tlsPropsMap
        invalidTlsPropsMap.remove(NiFiProperties.SECURITY_KEYSTORE_PASSWD)
        NiFiProperties mockInvalidTLSNiFiProperties = new NiFiProperties(new Properties(invalidTlsPropsMap))

        // Act
        OkHttpReplicationClient client = new OkHttpReplicationClient(mockNiFiProperties)
        OkHttpReplicationClient invalidTlsClient = new OkHttpReplicationClient(mockInvalidTLSNiFiProperties)
        OkHttpReplicationClient tlsClient = new OkHttpReplicationClient(mockTLSNiFiProperties)

        // Assert
        assert !client.isTLSConfigured()
        assert !invalidTlsClient.isTLSConfigured()
        assert tlsClient.isTLSConfigured()
    }
}
