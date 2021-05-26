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

import org.apache.nifi.registry.security.authorization.exception.AccessDeniedException
import org.apache.nifi.registry.security.authorization.resource.Authorizable
import org.apache.nifi.registry.security.authorization.resource.ResourceType
import org.apache.nifi.registry.service.AuthorizationService
import org.apache.nifi.registry.service.RegistryService
import org.apache.nifi.registry.web.security.authorization.HttpMethodAuthorizationRules
import org.apache.nifi.registry.web.security.authorization.ResourceAuthorizationFilter
import org.apache.nifi.registry.web.security.authorization.StandardHttpMethodAuthorizationRules
import org.springframework.http.HttpMethod
import org.springframework.mock.web.MockHttpServletRequest
import org.springframework.mock.web.MockHttpServletResponse
import spock.lang.Specification

import javax.servlet.FilterChain
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

class ResourceAuthorizationFilterSpec extends Specification {

    RegistryService registryService = Mock(RegistryService)
    AuthorizableLookup authorizableLookup = new StandardAuthorizableLookup(registryService)
    AuthorizationService mockAuthorizationService = Mock(AuthorizationService)
    FilterChain mockFilterChain = Mock(FilterChain)
    ResourceAuthorizationFilter.Builder resourceAuthorizationFilterBuilder

    // runs before every feature method
    def setup() {
        mockAuthorizationService.getAuthorizableLookup() >> authorizableLookup
        resourceAuthorizationFilterBuilder = ResourceAuthorizationFilter.builder().setAuthorizationService(mockAuthorizationService)
    }

    // runs after every feature method
    def cleanup() {
        //mockAuthorizationService = null
        //mockFilterChain = null
        resourceAuthorizationFilterBuilder = null
    }

    // runs before the first feature method
    def setupSpec() {}

    // runs after the last feature method
    def cleanupSpec() {}


    def "unsecured requests are allowed without an authorization check"() {

        setup:
        def resourceAuthorizationFilter = resourceAuthorizationFilterBuilder.addResourceType(ResourceType.Actuator).build()
        def httpServletRequest = createUnsecuredRequest()
        def httpServletResponse = createResponse()

        when: "doFilter() is called"
        resourceAuthorizationFilter.doFilter(httpServletRequest, httpServletResponse, mockFilterChain)

        then: "response is forwarded without authorization check"
        0 * mockAuthorizationService._
        1 * mockFilterChain.doFilter(_ as HttpServletRequest, _ as HttpServletResponse)

    }


    def "secure requests to an unguarded resource are allowed without an authorization check"() {

        setup:
        def resourceAuthorizationFilter = resourceAuthorizationFilterBuilder.addResourceType(ResourceType.Actuator).build()
        def httpServletRequest = createSecureRequest(HttpMethod.POST, ResourceType.Bucket)
        def httpServletResponse = createResponse()

        when: "doFilter() is called"
        resourceAuthorizationFilter.doFilter(httpServletRequest, httpServletResponse, mockFilterChain)

        then: "response is forwarded without authorization check"
        0 * mockAuthorizationService._
        1 * mockFilterChain.doFilter(_ as HttpServletRequest, _ as HttpServletResponse)

    }


    def "secure requests to an unguarded HTTP method are allowed without an authorization check"() {

        setup:
        HttpMethodAuthorizationRules rules = new StandardHttpMethodAuthorizationRules(EnumSet.of(HttpMethod.POST, HttpMethod.PUT, HttpMethod.DELETE))
        def resourceAuthorizationFilter = resourceAuthorizationFilterBuilder.addResourceType(ResourceType.Actuator, rules).build()
        def httpServletRequest = createSecureRequest(HttpMethod.GET, ResourceType.Actuator)
        def httpServletResponse = createResponse()

        when: "doFilter() is called"
        resourceAuthorizationFilter.doFilter(httpServletRequest, httpServletResponse, mockFilterChain)

        then: "response is forwarded without authorization check"
        0 * mockAuthorizationService._
        1 * mockFilterChain.doFilter(_ as HttpServletRequest, _ as HttpServletResponse)

    }


    def "secure requests matching resource configuration rules perform authorization check"() {

        setup:
        // Stubbing setup for mockAuthorizationService is done in the then block as we are also verifying interactions with mock
        def resourceAuthorizationFilter = resourceAuthorizationFilterBuilder.addResourceType(ResourceType.Actuator).build()
        def authorizedRequest = createSecureRequest(HttpMethod.GET, ResourceType.Actuator)
        def unauthorizedRequest = createSecureRequest(HttpMethod.POST, ResourceType.Actuator)
        def httpServletResponse = createResponse()


        when: "doFilter() is called with an authorized request"
        resourceAuthorizationFilter.doFilter(authorizedRequest, httpServletResponse, mockFilterChain)

        then: "response is forwarded after authorization check"
        1 * mockAuthorizationService.authorize(_ as Authorizable, RequestAction.READ) >> { allowAccess() }
        1 * mockFilterChain.doFilter(_ as HttpServletRequest, _ as HttpServletResponse)


        when: "doFilter() is called with an unauthorized request"
        resourceAuthorizationFilter.doFilter(unauthorizedRequest, httpServletResponse, mockFilterChain)

        then: "authorization check is performed and response is not forwarded"
        1 * mockAuthorizationService.authorize(_ as Authorizable, RequestAction.WRITE) >> { denyAccess() }
        0 * mockFilterChain.doFilter(*_)

    }

    static private HttpServletRequest createUnsecuredRequest() {
        HttpServletRequest req = new MockHttpServletRequest()
        req.setScheme("http")
        req.setSecure(false)
        return req
    }

    static private HttpServletRequest createSecureRequest(HttpMethod httpMethod, ResourceType resourceType) {
        HttpServletRequest req = new MockHttpServletRequest()
        req.setMethod(httpMethod.name())
        req.setScheme("https")
        req.setServletPath(resourceType.getValue())
        req.setSecure(true)
        return req
    }

    static private HttpServletResponse createResponse() {
        HttpServletResponse res = new MockHttpServletResponse()
        return res
    }

    static private void allowAccess() {
        // Do nothing (no thrown exception indicates access is allowed
    }

    static private void denyAccess() {
        throw new AccessDeniedException("This is an expected AccessDeniedException.")
    }

}
