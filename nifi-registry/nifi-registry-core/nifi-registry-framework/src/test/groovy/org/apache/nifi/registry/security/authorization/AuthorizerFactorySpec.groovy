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
package org.apache.nifi.registry.security.authorization

import org.apache.nifi.registry.extension.ExtensionClassLoader
import org.apache.nifi.registry.extension.ExtensionManager
import org.apache.nifi.registry.properties.NiFiRegistryProperties
import org.apache.nifi.registry.security.authorization.resource.ResourceFactory
import org.apache.nifi.registry.security.identity.IdentityMapper
import org.apache.nifi.registry.service.RegistryService
import spock.lang.Specification

import javax.sql.DataSource

class AuthorizerFactorySpec extends Specification {

    def mockProperties = Mock(NiFiRegistryProperties)
    def mockExtensionManager = Mock(ExtensionManager)
    def mockRegistryService = Mock(RegistryService)
    def mockDataSource = Mock(DataSource)
    def mockIdentityMapper = Mock(IdentityMapper)

    AuthorizerFactory authorizerFactory

    // runs before every feature method
    def setup() {
        mockExtensionManager.getExtensionClassLoader(_) >> new ExtensionClassLoader("/tmp", new URL[0],this.getClass().getClassLoader())
        mockProperties.getPropertyKeys() >> new HashSet<String>() // Called by IdentityMappingUtil.getIdentityMappings()

        authorizerFactory = new AuthorizerFactory(mockProperties, mockExtensionManager, null, mockRegistryService, mockDataSource, mockIdentityMapper)
    }

    // runs after every feature method
    def cleanup() {
        authorizerFactory = null
    }

    // runs before the first feature method
    def setupSpec() {}

    // runs after the last feature method
    def cleanupSpec() {}

    def "create default authorizer"() {

        setup: "properties indicate nifi-registry is unsecured"
        mockProperties.getProperty(NiFiRegistryProperties.WEB_HTTPS_PORT) >> ""

        when: "getAuthorizer() is first called"
        def authorizer = authorizerFactory.getAuthorizer()

        then: "the default authorizer is returned"
        authorizer != null

        and: "any authorization request made to that authorizer is approved"
        def authorizationResult = authorizer.authorize(getTestAuthorizationRequest())
        authorizationResult.result == AuthorizationResult.Result.Approved

    }

    def "create file-backed authorizer"() {

        setup:
        setMockPropsAuthorizersConfig("src/test/resources/security/authorizers-good-file-providers.xml", "managed-authorizer")

        when: "getAuthorizer() is first called"
        def authorizer = authorizerFactory.getAuthorizer()

        then: "an authorizer is returned with the expected providers"
        authorizer != null
        authorizer instanceof ManagedAuthorizer
        def apProvider = ((ManagedAuthorizer) authorizer).getAccessPolicyProvider()
        apProvider instanceof ConfigurableAccessPolicyProvider
        def ugProvider = ((ConfigurableAccessPolicyProvider) apProvider).getUserGroupProvider()
        ugProvider instanceof ConfigurableUserGroupProvider

    }

    def "invalid authorizer configuration fails"() {

        when: "a bad configuration is provided and getAuthorizer() is called"
        setMockPropsAuthorizersConfig(authorizersConfigFile, selectedAuthorizer)
        authorizerFactory = new AuthorizerFactory(mockProperties, mockExtensionManager, null, mockRegistryService, mockDataSource, mockIdentityMapper)
        authorizerFactory.getAuthorizer()

        then: "expect an exception"
        def e = thrown AuthorizerFactoryException
        e.message =~ expectedExceptionMessage || e.getCause().getMessage() =~ expectedExceptionMessage

        where:
        authorizersConfigFile                                                    | selectedAuthorizer        | expectedExceptionMessage
        "src/test/resources/security/authorizers-good-file-providers.xml"        | ""                        | "When running securely, the authorizer identifier must be specified in the nifi-registry.properties file."
        "src/test/resources/security/authorizers-good-file-providers.xml"        | "non-existent-authorizer" | "The specified authorizer 'non-existent-authorizer' could not be found."
        "src/test/resources/security/authorizers-bad-ug-provider-ids.xml"        | "managed-authorizer"      | "Duplicate User Group Provider identifier in Authorizers configuration"
        "src/test/resources/security/authorizers-bad-ap-provider-ids.xml"        | "managed-authorizer"      | "Duplicate Access Policy Provider identifier in Authorizers configuration"
        "src/test/resources/security/authorizers-bad-authorizer-ids.xml"         | "managed-authorizer"      | "Duplicate Authorizer identifier in Authorizers configuration"
        "src/test/resources/security/authorizers-bad-composite.xml"              | "managed-authorizer"      | "Duplicate provider in Composite User Group Provider configuration"
        "src/test/resources/security/authorizers-bad-configurable-composite.xml" | "managed-authorizer"      | "Duplicate provider in Composite Configurable User Group Provider configuration"

    }

    // Helper methods

    private void setMockPropsAuthorizersConfig(String filePath, String authorizer = "managed-authorizer") {
        mockProperties.getProperty(NiFiRegistryProperties.WEB_HTTPS_PORT) >> "443"
        mockProperties.getSslPort() >> 443 // required to be non-null to create authorizer
        mockProperties.getProperty(NiFiRegistryProperties.SECURITY_AUTHORIZERS_CONFIGURATION_FILE) >> filePath
        mockProperties.getAuthorizersConfigurationFile() >> new File(filePath)
        mockProperties.getProperty(NiFiRegistryProperties.SECURITY_AUTHORIZER) >> authorizer
    }

    private static AuthorizationRequest getTestAuthorizationRequest() {
        return new AuthorizationRequest.Builder()
                .resource(ResourceFactory.getBucketsResource())
                .action(RequestAction.WRITE)
                .accessAttempt(false)
                .anonymous(true)
                .build()
    }

}
