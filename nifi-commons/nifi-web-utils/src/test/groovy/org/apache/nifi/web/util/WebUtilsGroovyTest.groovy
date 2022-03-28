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
package org.apache.nifi.web.util

import org.apache.http.conn.ssl.DefaultHostnameVerifier
import org.glassfish.jersey.client.ClientConfig
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import sun.security.tools.keytool.CertAndKeyGen
import sun.security.x509.X500Name

import javax.net.ssl.HostnameVerifier
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLPeerUnverifiedException
import javax.servlet.http.HttpServletRequest
import javax.ws.rs.client.Client
import javax.ws.rs.core.UriBuilderException
import java.security.cert.X509Certificate

import static org.junit.jupiter.api.Assertions.assertThrows
import static org.junit.jupiter.api.Assertions.assertTrue

class WebUtilsGroovyTest {
    static final String PCP_HEADER = "X-ProxyContextPath"
    static final String FC_HEADER = "X-Forwarded-Context"
    static final String FP_HEADER = "X-Forwarded-Prefix"

    static final String ALLOWED_PATH = "/some/context/path"

    HttpServletRequest mockRequest(Map keys) {
        HttpServletRequest mockRequest = [
                getContextPath: { ->
                    "default/path"
                },
                getHeader     : { String k ->
                    switch (k) {
                        case PCP_HEADER:
                            return keys["proxy"]
                            break
                        case FC_HEADER:
                            return keys["forward"]
                            break
                        case FP_HEADER:
                            return keys["prefix"]
                            break
                        default:
                            return ""
                    }
                }] as HttpServletRequest
        mockRequest
    }

    @Test
    void testShouldDetermineCorrectContextPathWhenPresent() throws Exception {
        // Arrange
        final String CORRECT_CONTEXT_PATH = ALLOWED_PATH
        final String WRONG_CONTEXT_PATH = "this/is/a/bad/path"

        // Variety of requests with different ordering of context paths (the correct one is always "some/context/path"
        HttpServletRequest proxyRequest = mockRequest([proxy: CORRECT_CONTEXT_PATH])
        HttpServletRequest forwardedRequest = mockRequest([forward: CORRECT_CONTEXT_PATH])
        HttpServletRequest prefixRequest = mockRequest([prefix: CORRECT_CONTEXT_PATH])
        HttpServletRequest proxyBeforeForwardedRequest = mockRequest([proxy: CORRECT_CONTEXT_PATH, forward: WRONG_CONTEXT_PATH])
        HttpServletRequest proxyBeforePrefixRequest = mockRequest([proxy: CORRECT_CONTEXT_PATH, prefix: WRONG_CONTEXT_PATH])
        HttpServletRequest forwardBeforePrefixRequest = mockRequest([forward: CORRECT_CONTEXT_PATH, prefix: WRONG_CONTEXT_PATH])
        List<HttpServletRequest> requests = [proxyRequest, forwardedRequest, prefixRequest, proxyBeforeForwardedRequest,
                                             proxyBeforePrefixRequest, forwardBeforePrefixRequest]

        // Act
        requests.each { HttpServletRequest request ->
            String determinedContextPath = WebUtils.determineContextPath(request)

            // Assert
            assert determinedContextPath == CORRECT_CONTEXT_PATH
        }
    }

    @Test
    void testShouldDetermineCorrectContextPathWhenAbsent() throws Exception {
        // Arrange
        final String CORRECT_CONTEXT_PATH = ""

        // Variety of requests with different ordering of non-existent context paths (the correct one is always ""
        HttpServletRequest proxyRequest = mockRequest([proxy: ""])
        HttpServletRequest proxySpacesRequest = mockRequest([proxy: "   "])
        HttpServletRequest forwardedRequest = mockRequest([forward: ""])
        HttpServletRequest forwardedSpacesRequest = mockRequest([forward: "   "])
        HttpServletRequest prefixRequest = mockRequest([prefix: ""])
        HttpServletRequest prefixSpacesRequest = mockRequest([prefix: "   "])
        HttpServletRequest proxyBeforeForwardedOrPrefixRequest = mockRequest([proxy: "", forward: "", prefix: ""])
        HttpServletRequest proxyBeforeForwardedOrPrefixSpacesRequest = mockRequest([proxy: "   ", forward: "   ", prefix: "   "])
        List<HttpServletRequest> requests = [proxyRequest, proxySpacesRequest, forwardedRequest, forwardedSpacesRequest, prefixRequest, prefixSpacesRequest,
                                             proxyBeforeForwardedOrPrefixRequest, proxyBeforeForwardedOrPrefixSpacesRequest]

        // Act
        requests.each { HttpServletRequest request ->
            String determinedContextPath = WebUtils.determineContextPath(request)

            // Assert
            assert determinedContextPath == CORRECT_CONTEXT_PATH
        }
    }

    @Test
    void testShouldNormalizeContextPath() throws Exception {
        // Arrange
        final String CORRECT_CONTEXT_PATH = ALLOWED_PATH
        final String TRIMMED_PATH = ALLOWED_PATH[1..-1] // Trims leading /

        // Variety of different context paths (the correct one is always "/some/context/path")
        List<String> contextPaths = ["/$TRIMMED_PATH", "/" + TRIMMED_PATH, TRIMMED_PATH, TRIMMED_PATH + "/"]

        // Act
        contextPaths.each { String contextPath ->
            String normalizedContextPath = WebUtils.normalizeContextPath(contextPath)

            // Assert
            assert normalizedContextPath == CORRECT_CONTEXT_PATH
        }
    }

    @Test
    void testVerifyContextPathShouldAllowContextPathHeaderIfInAllowList() throws Exception {
        WebUtils.verifyContextPath(Arrays.asList(ALLOWED_PATH), ALLOWED_PATH)
    }

    @Test
    void testVerifyContextPathShouldAllowContextPathHeaderIfInMultipleAllowLists() throws Exception {
        WebUtils.verifyContextPath(Arrays.asList(ALLOWED_PATH, ALLOWED_PATH.reverse()), ALLOWED_PATH)
    }

    @Test
    void testVerifyContextPathShouldAllowContextPathHeaderIfBlank() throws Exception {
        def emptyContextPaths = ["", "  ", "\t", null]
        emptyContextPaths.each { String contextPath ->
            WebUtils.verifyContextPath(Arrays.asList(ALLOWED_PATH), contextPath)
        }
    }

    @Test
    void testVerifyContextPathShouldBlockContextPathHeaderIfNotAllowed() throws Exception {
        def invalidContextPaths = ["/other/path", "localhost", "/../trying/to/escape"]

        invalidContextPaths.each { String contextPath ->
            assertThrows(UriBuilderException.class, () -> WebUtils.verifyContextPath(Arrays.asList(ALLOWED_PATH), contextPath))
        }
    }

    @Test
    void testHostnameVerifierType() {
        // Arrange
        SSLContext sslContext = Mockito.mock(SSLContext.class)
        final ClientConfig clientConfig = new ClientConfig()

        // Act
        Client client = WebUtils.createClient(clientConfig, sslContext)
        HostnameVerifier hostnameVerifier = client.getHostnameVerifier()

        // Assert
        assertTrue(hostnameVerifier instanceof DefaultHostnameVerifier)
    }

    @Test
    void testHostnameVerifierWildcard() {
        // Arrange
        final String EXPECTED_DN = "CN=*.apache.com,OU=Security,O=Apache,ST=CA,C=US"
        final String hostname = "nifi.apache.com"
        X509Certificate cert = generateCertificate(EXPECTED_DN)
        SSLContext sslContext = Mockito.mock(SSLContext.class)
        final ClientConfig clientConfig = new ClientConfig()

        // Act
        Client client = WebUtils.createClient(clientConfig, sslContext)
        DefaultHostnameVerifier hostnameVerifier = (DefaultHostnameVerifier) client.getHostnameVerifier()

        // Verify
        hostnameVerifier.verify(hostname, cert)
    }

    @Test
    void testHostnameVerifierDNWildcardFourthLevelDomain() {
        // Arrange
        final String EXPECTED_DN = "CN=*.nifi.apache.org,OU=Security,O=Apache,ST=CA,C=US"
        final String clientHostname = "client.nifi.apache.org"
        final String serverHostname = "server.nifi.apache.org"
        X509Certificate cert = generateCertificate(EXPECTED_DN)
        SSLContext sslContext = Mockito.mock(SSLContext.class)
        final ClientConfig clientConfig = new ClientConfig()


        // Act
        Client client = WebUtils.createClient(clientConfig, sslContext)
        DefaultHostnameVerifier hostnameVerifier = client.getHostnameVerifier()

        // Verify
        hostnameVerifier.verify(clientHostname, cert)
        hostnameVerifier.verify(serverHostname, cert)
    }

    @Test
    void testHostnameVerifierDomainLevelMismatch() {
        // Arrange
        final String EXPECTED_DN = "CN=*.nifi.apache.org,OU=Security,O=Apache,ST=CA,C=US"
        final String hostname = "nifi.apache.org"
        X509Certificate cert = generateCertificate(EXPECTED_DN)
        SSLContext sslContext = Mockito.mock(SSLContext.class)
        final ClientConfig clientConfig = new ClientConfig()

        // Act
        Client client = WebUtils.createClient(clientConfig, sslContext)
        DefaultHostnameVerifier hostnameVerifier = client.getHostnameVerifier()

        assertThrows(SSLPeerUnverifiedException.class, () -> hostnameVerifier.verify(hostname, cert))
    }

    @Test
    void testHostnameVerifierEmptyHostname() {
        // Arrange
        final String EXPECTED_DN = "CN=nifi.apache.org,OU=Security,O=Apache,ST=CA,C=US"
        final String hostname = ""
        X509Certificate cert = generateCertificate(EXPECTED_DN)
        SSLContext sslContext = Mockito.mock(SSLContext.class)
        final ClientConfig clientConfig = new ClientConfig()

        // Act
        Client client = WebUtils.createClient(clientConfig, sslContext)
        DefaultHostnameVerifier hostnameVerifier = client.getHostnameVerifier()

        assertThrows(SSLPeerUnverifiedException.class, () -> hostnameVerifier.verify(hostname, cert))
    }

    @Test
    void testHostnameVerifierDifferentSubdomain() {
        // Arrange
        final String EXPECTED_DN = "CN=nifi.apache.org,OU=Security,O=Apache,ST=CA,C=US"
        final String hostname = "egg.apache.org"
        X509Certificate cert = generateCertificate(EXPECTED_DN)
        SSLContext sslContext = Mockito.mock(SSLContext.class)
        final ClientConfig clientConfig = new ClientConfig()

        // Act
        Client client = WebUtils.createClient(clientConfig, sslContext)
        DefaultHostnameVerifier hostnameVerifier = client.getHostnameVerifier()

        assertThrows(SSLPeerUnverifiedException.class, () -> hostnameVerifier.verify(hostname, cert))
    }

    @Test
    void testHostnameVerifierDifferentTLD() {
        // Arrange
        final String EXPECTED_DN = "CN=nifi.apache.org,OU=Security,O=Apache,ST=CA,C=US"
        final String hostname = "nifi.apache.com"
        X509Certificate cert = generateCertificate(EXPECTED_DN)
        SSLContext sslContext = Mockito.mock(SSLContext.class)
        final ClientConfig clientConfig = new ClientConfig()

        // Act
        Client client = WebUtils.createClient(clientConfig, sslContext)
        DefaultHostnameVerifier hostnameVerifier = client.getHostnameVerifier()

        assertThrows(SSLPeerUnverifiedException.class, () -> hostnameVerifier.verify(hostname, cert))
    }

    @Test
    void testHostnameVerifierWildcardTLD() {
        // Arrange
        final String EXPECTED_DN = "CN=nifi.apache.*,OU=Security,O=Apache,ST=CA,C=US"
        final String comTLDhostname = "nifi.apache.com"
        final String orgTLDHostname = "nifi.apache.org"
        X509Certificate cert = generateCertificate(EXPECTED_DN)
        SSLContext sslContext = Mockito.mock(SSLContext.class)
        final ClientConfig clientConfig = new ClientConfig()

        // Act
        Client client = WebUtils.createClient(clientConfig, sslContext)
        DefaultHostnameVerifier hostnameVerifier = client.getHostnameVerifier()

        // Verify
        hostnameVerifier.verify(comTLDhostname, cert)
        hostnameVerifier.verify(orgTLDHostname, cert)
    }

    @Test
    void testHostnameVerifierWildcardDomain() {
        // Arrange
        final String EXPECTED_DN = "CN=nifi.*.com,OU=Security,O=Apache,ST=CA,C=US"
        final String hostname = "nifi.apache.com"
        X509Certificate cert = generateCertificate(EXPECTED_DN)
        SSLContext sslContext = Mockito.mock(SSLContext.class)
        final ClientConfig clientConfig = new ClientConfig()

        // Act
        Client client = WebUtils.createClient(clientConfig, sslContext)
        DefaultHostnameVerifier hostnameVerifier = client.getHostnameVerifier()

        // Verify
        hostnameVerifier.verify(hostname, cert)
    }

    X509Certificate generateCertificate(String DN) {
         CertAndKeyGen certGenerator = new CertAndKeyGen("RSA", "SHA256WithRSA", null)
         certGenerator.generate(2048)

         long validityPeriod = (long) 365 * 24 * 60 * 60 // 1 YEAR
         X509Certificate cert = certGenerator.getSelfCertificate(new X500Name(DN), validityPeriod)
         return cert
    }
}
