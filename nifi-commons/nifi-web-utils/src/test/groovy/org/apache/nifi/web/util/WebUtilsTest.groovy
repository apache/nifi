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

import org.glassfish.jersey.client.ClientConfig
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.Mockito
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.http.conn.ssl.DefaultHostnameVerifier
import sun.security.tools.keytool.CertAndKeyGen
import sun.security.x509.X500Name
import javax.net.ssl.SSLPeerUnverifiedException
import javax.servlet.http.HttpServletRequest
import javax.ws.rs.core.UriBuilderException
import javax.ws.rs.client.Client
import javax.net.ssl.SSLContext
import javax.net.ssl.HostnameVerifier
import java.security.cert.X509Certificate


@RunWith(JUnit4.class)
class WebUtilsTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(WebUtilsTest.class)

    static final String PCP_HEADER = "X-ProxyContextPath"
    static final String FC_HEADER = "X-Forwarded-Context"
    static final String FP_HEADER = "X-Forwarded-Prefix"

    static final String WHITELISTED_PATH = "/some/context/path"
    private static final String OCSP_REQUEST_CONTENT_TYPE = "application/ocsp-request"

    @BeforeClass
    static void setUpOnce() throws Exception {
        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    void setUp() throws Exception {
    }

    @After
    void tearDown() throws Exception {
    }

    HttpServletRequest mockRequest(Map keys) {
        HttpServletRequest mockRequest = [
                getContextPath: { ->
                    logger.mock("Request.getContextPath() -> default/path")
                    "default/path"
                },
                getHeader     : { String k ->
                    logger.mock("Request.getHeader($k) -> ${keys}")
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
        final String CORRECT_CONTEXT_PATH = WHITELISTED_PATH
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
            logger.info("Determined context path: ${determinedContextPath}")

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
            logger.info("Determined context path: ${determinedContextPath}")

            // Assert
            assert determinedContextPath == CORRECT_CONTEXT_PATH
        }
    }

    @Test
    void testShouldNormalizeContextPath() throws Exception {
        // Arrange
        final String CORRECT_CONTEXT_PATH = WHITELISTED_PATH
        final String TRIMMED_PATH = WHITELISTED_PATH[1..-1] // Trims leading /

        // Variety of different context paths (the correct one is always "/some/context/path")
        List<String> contextPaths = ["/$TRIMMED_PATH", "/" + TRIMMED_PATH, TRIMMED_PATH, TRIMMED_PATH + "/"]

        // Act
        contextPaths.each { String contextPath ->
            String normalizedContextPath = WebUtils.normalizeContextPath(contextPath)
            logger.info("Normalized context path: ${normalizedContextPath} <- ${contextPath}")

            // Assert
            assert normalizedContextPath == CORRECT_CONTEXT_PATH
        }
    }

    @Test
    void testGetResourcePathShouldBlockContextPathHeaderIfNotInWhitelist() throws Exception {
        // Arrange
        logger.info("Whitelisted path(s): ")

        HttpServletRequest requestWithProxyHeader = mockRequest([proxy: "any/context/path"])
        HttpServletRequest requestWithProxyAndForwardHeader = mockRequest([proxy: "any/context/path", forward: "any/other/context/path"])
        HttpServletRequest requestWithProxyAndForwardAndPrefixHeader = mockRequest([proxy : "any/context/path", forward: "any/other/context/path",
                                                                                    prefix: "any/other/prefix/path"])
        List<HttpServletRequest> requests = [requestWithProxyHeader, requestWithProxyAndForwardHeader, requestWithProxyAndForwardAndPrefixHeader]

        // Act
        requests.each { HttpServletRequest request ->
            def msg = shouldFail(UriBuilderException) {
                String generatedResourcePath = WebUtils.getResourcePath(new URI('https://nifi.apache.org/actualResource'), request, "")
                logger.unexpected("Generated Resource Path: ${generatedResourcePath}")
            }

            // Assert
            logger.expected(msg)
            assert msg =~ "The provided context path \\[.*\\] was not whitelisted \\[\\]"
        }
    }

    @Test
    void testGetResourcePathShouldAllowContextPathHeaderIfInWhitelist() throws Exception {
        // Arrange
        logger.info("Whitelisted path(s): ${WHITELISTED_PATH}")

        HttpServletRequest requestWithProxyHeader = mockRequest([proxy: "some/context/path"])
        HttpServletRequest requestWithForwardHeader = mockRequest([forward: "some/context/path"])
        HttpServletRequest requestWithProxyAndForwardHeader = mockRequest([proxy: "some/context/path", forward: "any/other/context/path"])
        HttpServletRequest requestWithProxyAndForwardAndPrefixHeader = mockRequest([proxy: "some/context/path", forward: "any/other/context/path",
                                                                                    prefix: "any/other/prefix/path"])
        List<HttpServletRequest> requests = [requestWithProxyHeader, requestWithForwardHeader, requestWithProxyAndForwardHeader,
                                             requestWithProxyAndForwardAndPrefixHeader]

        // Act
        requests.each { HttpServletRequest request ->
            String generatedResourcePath = WebUtils.getResourcePath(new URI('https://nifi.apache.org/actualResource'), request, WHITELISTED_PATH)
            logger.info("Generated Resource Path: ${generatedResourcePath}")

            // Assert
            assert generatedResourcePath == "${WHITELISTED_PATH}/actualResource"
        }
    }

    @Test
    void testGetResourcePathShouldAllowContextPathHeaderIfElementInMultipleWhitelist() throws Exception {
        // Arrange
        String multipleWhitelistedPaths = [WHITELISTED_PATH, "/another/path", "/a/third/path", "/a/prefix/path"].join(",")
        logger.info("Whitelisted path(s): ${multipleWhitelistedPaths}")

        final List<String> VALID_RESOURCE_PATHS = multipleWhitelistedPaths.split(",").collect { "$it/actualResource" }

        HttpServletRequest requestWithProxyHeader = mockRequest([proxy: "some/context/path"])
        HttpServletRequest requestWithForwardHeader = mockRequest([forward: "another/path"])
        HttpServletRequest requestWithPrefixHeader = mockRequest([prefix: "a/prefix/path"])
        HttpServletRequest requestWithProxyAndForwardHeader = mockRequest([proxy: "a/third/path", forward: "any/other/context/path"])
        HttpServletRequest requestWithProxyAndForwardAndPrefixHeader = mockRequest([proxy : "a/third/path", forward: "any/other/context/path",
                                                                                    prefix: "any/other/prefix/path"])
        List<HttpServletRequest> requests = [requestWithProxyHeader, requestWithForwardHeader, requestWithProxyAndForwardHeader,
                                             requestWithPrefixHeader, requestWithProxyAndForwardAndPrefixHeader]

        // Act
        requests.each { HttpServletRequest request ->
            String generatedResourcePath = WebUtils.getResourcePath(new URI('https://nifi.apache.org/actualResource'), request, multipleWhitelistedPaths)
            logger.info("Generated Resource Path: ${generatedResourcePath}")

            // Assert
            assert VALID_RESOURCE_PATHS.any { it == generatedResourcePath }
        }
    }

    @Test
    void testVerifyContextPathShouldAllowContextPathHeaderIfInWhitelist() throws Exception {
        // Arrange
        logger.info("Whitelisted path(s): ${WHITELISTED_PATH}")
        String contextPath = WHITELISTED_PATH

        // Act
        logger.info("Testing [${contextPath}] against ${WHITELISTED_PATH}")
        WebUtils.verifyContextPath(WHITELISTED_PATH, contextPath)
        logger.info("Verified [${contextPath}]")

        // Assert
        // Would throw exception if invalid
    }

    @Test
    void testVerifyContextPathShouldAllowContextPathHeaderIfInMultipleWhitelist() throws Exception {
        // Arrange
        String multipleWhitelist = [WHITELISTED_PATH, WebUtils.normalizeContextPath(WHITELISTED_PATH.reverse())].join(",")
        logger.info("Whitelisted path(s): ${multipleWhitelist}")
        String contextPath = WHITELISTED_PATH

        // Act
        logger.info("Testing [${contextPath}] against ${multipleWhitelist}")
        WebUtils.verifyContextPath(multipleWhitelist, contextPath)
        logger.info("Verified [${contextPath}]")

        // Assert
        // Would throw exception if invalid
    }

    @Test
    void testVerifyContextPathShouldAllowContextPathHeaderIfBlank() throws Exception {
        // Arrange
        logger.info("Whitelisted path(s): ${WHITELISTED_PATH}")

        def emptyContextPaths = ["", "  ", "\t", null]

        // Act
        emptyContextPaths.each { String contextPath ->
            logger.info("Testing [${contextPath}] against ${WHITELISTED_PATH}")
            WebUtils.verifyContextPath(WHITELISTED_PATH, contextPath)
            logger.info("Verified [${contextPath}]")

            // Assert
            // Would throw exception if invalid
        }
    }

    @Test
    void testVerifyContextPathShouldBlockContextPathHeaderIfNotAllowed() throws Exception {
        // Arrange
        logger.info("Whitelisted path(s): ${WHITELISTED_PATH}")

        def invalidContextPaths = ["/other/path", "somesite.com", "/../trying/to/escape"]

        // Act
        invalidContextPaths.each { String contextPath ->
            logger.info("Testing [${contextPath}] against ${WHITELISTED_PATH}")
            def msg = shouldFail(UriBuilderException) {
                WebUtils.verifyContextPath(WHITELISTED_PATH, contextPath)
                logger.info("Verified [${contextPath}]")
            }

            // Assert
            logger.expected(msg)
            assert msg =~ " was not whitelisted "
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

    @Test(expected = SSLPeerUnverifiedException)
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

        // Verify
        hostnameVerifier.verify(hostname, cert)
    }

    @Test(expected = SSLPeerUnverifiedException)
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

        // Verify
        hostnameVerifier.verify(hostname, cert)
    }

    @Test(expected = SSLPeerUnverifiedException)
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

        // Verify
        hostnameVerifier.verify(hostname, cert)
    }

    @Test(expected = SSLPeerUnverifiedException)
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

        // Verify
        hostnameVerifier.verify(hostname, cert)
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
