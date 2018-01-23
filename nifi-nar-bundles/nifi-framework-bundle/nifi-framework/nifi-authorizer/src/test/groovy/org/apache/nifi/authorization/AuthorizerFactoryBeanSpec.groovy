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
package org.apache.nifi.authorization

import org.apache.nifi.authorization.resource.ResourceFactory
import org.apache.nifi.util.NiFiProperties
import spock.lang.Specification

class AuthorizerFactoryBeanSpec extends Specification {

    def mockProperties = Mock(NiFiProperties)

    AuthorizerFactoryBean authorizerFactoryBean

    // runs before every feature method
    def setup() {
        authorizerFactoryBean = new AuthorizerFactoryBean()
        authorizerFactoryBean.setProperties(mockProperties)
    }

    // runs after every feature method
    def cleanup() {}

    // runs before the first feature method
    def setupSpec() {}

    // runs after the last feature method
    def cleanupSpec() {}

    def "create default authorizer"() {

        setup: "properties indicate nifi-registry is unsecured"
        mockProperties.getProperty(NiFiProperties.WEB_HTTPS_PORT) >> ""

        when: "getAuthorizer() is first called"
        def authorizer = (Authorizer) authorizerFactoryBean.getObject()

        then: "the default authorizer is returned"
        authorizer != null

        and: "any authorization request made to that authorizer is approved"
        def authorizationResult = authorizer.authorize(getTestAuthorizationRequest())
        authorizationResult.result == AuthorizationResult.Result.Approved

    }

    // Helper methods

    private static AuthorizationRequest getTestAuthorizationRequest() {
        return new AuthorizationRequest.Builder()
                .resource(ResourceFactory.getFlowResource())
                .action(RequestAction.WRITE)
                .accessAttempt(false)
                .anonymous(true)
                .build()
    }

}
