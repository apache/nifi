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
package org.apache.nifi.registry.security.authorization;

import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.security.authorization.exception.AccessDeniedException;
import org.apache.nifi.registry.security.authorization.resource.Authorizable;
import org.apache.nifi.registry.security.authorization.resource.ResourceFactory;
import org.apache.nifi.registry.security.authorization.user.NiFiUser;
import org.apache.nifi.registry.security.authorization.user.StandardNiFiUser;
import org.apache.nifi.registry.service.RegistryService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestStandardAuthorizableLookup {

    private static final NiFiUser USER_NO_PROXY_CHAIN = new StandardNiFiUser.Builder()
            .identity("user1")
            .build();

    private static final NiFiUser USER_WITH_PROXY_CHAIN = new StandardNiFiUser.Builder()
            .identity("user1")
            .chain(new StandardNiFiUser.Builder().identity("CN=localhost, OU=NIFI").build())
            .build();

    private Authorizer authorizer;
    private RegistryService registryService;
    private AuthorizableLookup authorizableLookup;

    private Bucket bucketPublic;
    private Bucket bucketNotPublic;

    @Before
    public void setup() {
        authorizer = mock(Authorizer.class);
        registryService = mock(RegistryService.class);
        authorizableLookup = new StandardAuthorizableLookup(registryService);

        bucketPublic = new Bucket();
        bucketPublic.setIdentifier(UUID.randomUUID().toString());
        bucketPublic.setName("Public Bucket");
        bucketPublic.setAllowPublicRead(true);

        bucketNotPublic = new Bucket();
        bucketNotPublic.setIdentifier(UUID.randomUUID().toString());
        bucketNotPublic.setName("Non Public Bucket");
        bucketNotPublic.setAllowPublicRead(false);

        when(registryService.getBucket(bucketPublic.getIdentifier())).thenReturn(bucketPublic);
        when(registryService.getBucket(bucketNotPublic.getIdentifier())).thenReturn(bucketNotPublic);
    }

    // Test check method for Bucket Authorizable

    @Test
    public void testCheckReadPublicBucketWithNoProxyChain() {
        final Authorizable bucketAuthorizable = authorizableLookup.getBucketAuthorizable(bucketPublic.getIdentifier());
        final AuthorizationResult result = bucketAuthorizable.checkAuthorization(authorizer, RequestAction.READ, USER_NO_PROXY_CHAIN);
        assertNotNull(result);
        assertEquals(AuthorizationResult.Result.Approved, result.getResult());

        // Should never call authorizer because resource is public
        verify(authorizer, times(0)).authorize(any(AuthorizationRequest.class));
    }

    @Test
    public void testCheckReadPublicBucketWithProxyChain() {
        final Authorizable bucketAuthorizable = authorizableLookup.getBucketAuthorizable(bucketPublic.getIdentifier());
        final AuthorizationResult result = bucketAuthorizable.checkAuthorization(authorizer, RequestAction.READ, USER_WITH_PROXY_CHAIN);
        assertNotNull(result);
        assertEquals(AuthorizationResult.Result.Approved, result.getResult());

        // Should never call authorizer because resource is public
        verify(authorizer, times(0)).authorize(any(AuthorizationRequest.class));
    }

    @Test
    public void testCheckWritePublicBucketWithUnauthorizedUserAndNoProxyChain() {
        final RequestAction action = RequestAction.WRITE;

        // first request will be to the specific bucket
        final AuthorizationRequest expectedBucketAuthorizationRequest = getBucketAuthorizationRequest(
                bucketPublic.getIdentifier(), action, USER_NO_PROXY_CHAIN);

        when(authorizer.authorize(argThat(new AuthorizationRequestMatcher(expectedBucketAuthorizationRequest))))
                .thenReturn(AuthorizationResult.denied());

        // second request will go to parent of /buckets
        final AuthorizationRequest expectedBucketsAuthorizationRequest = getBucketsAuthorizationRequest(action, USER_NO_PROXY_CHAIN);

        when(authorizer.authorize(argThat(new AuthorizationRequestMatcher(expectedBucketsAuthorizationRequest))))
                .thenReturn(AuthorizationResult.denied());

        // should reach authorizer and return denied
        final Authorizable bucketAuthorizable = authorizableLookup.getBucketAuthorizable(bucketPublic.getIdentifier());
        final AuthorizationResult result = bucketAuthorizable.checkAuthorization(authorizer, action, USER_NO_PROXY_CHAIN);
        assertNotNull(result);
        assertEquals(AuthorizationResult.Result.Denied, result.getResult());

        // Should call authorizer twice for specific bucket and top-level /buckets
        verify(authorizer, times(2)).authorize(any(AuthorizationRequest.class));
    }

    @Test
    public void testCheckWritePublicBucketWithUnauthorizedProxyChain() {
        final RequestAction action = RequestAction.WRITE;

        // first request will be to authorize the proxy
        final AuthorizationRequest expectedProxyAuthorizationRequest = getProxyAuthorizationRequest(action, USER_WITH_PROXY_CHAIN.getChain());

        when(authorizer.authorize(argThat(new AuthorizationRequestMatcher(expectedProxyAuthorizationRequest))))
                .thenReturn(AuthorizationResult.denied());

        // the authorization of the proxy chain should return denied
        final Authorizable bucketAuthorizable = authorizableLookup.getBucketAuthorizable(bucketPublic.getIdentifier());
        final AuthorizationResult result = bucketAuthorizable.checkAuthorization(authorizer, action, USER_WITH_PROXY_CHAIN);
        assertNotNull(result);
        assertEquals(AuthorizationResult.Result.Denied, result.getResult());

        // Should never call authorizer once for /proxy and then return denied
        verify(authorizer, times(1)).authorize(any(AuthorizationRequest.class));
    }

    @Test
    public void testCheckWritePublicBucketWithUnauthorizedUserAndAuthorizedProxyChain() {
        final NiFiUser user = USER_WITH_PROXY_CHAIN;
        final RequestAction action = RequestAction.WRITE;

        // first request will be to authorize the proxy
        final AuthorizationRequest expectedProxyAuthorizationRequest = getProxyAuthorizationRequest(action, user.getChain());

        when(authorizer.authorize(argThat(new AuthorizationRequestMatcher(expectedProxyAuthorizationRequest))))
                .thenReturn(AuthorizationResult.approved());

        // second request will be to the specific bucket
        final AuthorizationRequest expectedBucketAuthorizationRequest = getBucketAuthorizationRequest(
                bucketPublic.getIdentifier(), action, user);

        when(authorizer.authorize(argThat(new AuthorizationRequestMatcher(expectedBucketAuthorizationRequest))))
                .thenReturn(AuthorizationResult.denied());

        // third request will go to parent of /buckets
        final AuthorizationRequest expectedBucketsAuthorizationRequest = getBucketsAuthorizationRequest(action, user);

        when(authorizer.authorize(argThat(new AuthorizationRequestMatcher(expectedBucketsAuthorizationRequest))))
                .thenReturn(AuthorizationResult.denied());

        // the authorization of the proxy chain should return denied
        final Authorizable bucketAuthorizable = authorizableLookup.getBucketAuthorizable(bucketPublic.getIdentifier());
        final AuthorizationResult result = bucketAuthorizable.checkAuthorization(authorizer, action, user);
        assertNotNull(result);
        assertEquals(AuthorizationResult.Result.Denied, result.getResult());

        // Should call authorizer three time for /proxy, /bucket/{id}, and /buckets
        verify(authorizer, times(3)).authorize(any(AuthorizationRequest.class));
    }

    @Test
    public void testCheckWritePublicBucketWithAuthorizedUserAndAuthorizedProxyChain() {
        final NiFiUser user = USER_WITH_PROXY_CHAIN;
        final RequestAction action = RequestAction.WRITE;

        // first request will be to authorize the proxy
        final AuthorizationRequest expectedProxyAuthorizationRequest = getProxyAuthorizationRequest(action, user.getChain());

        when(authorizer.authorize(argThat(new AuthorizationRequestMatcher(expectedProxyAuthorizationRequest))))
                .thenReturn(AuthorizationResult.approved());

        // second request will be to the specific bucket
        final AuthorizationRequest expectedBucketAuthorizationRequest = getBucketAuthorizationRequest(
                bucketPublic.getIdentifier(), action, user);

        when(authorizer.authorize(argThat(new AuthorizationRequestMatcher(expectedBucketAuthorizationRequest))))
                .thenReturn(AuthorizationResult.approved());

        // the authorization should all return approved
        final Authorizable bucketAuthorizable = authorizableLookup.getBucketAuthorizable(bucketPublic.getIdentifier());
        final AuthorizationResult result = bucketAuthorizable.checkAuthorization(authorizer, action, user);
        assertNotNull(result);
        assertEquals(AuthorizationResult.Result.Approved, result.getResult());

        // Should call authorizer two times for /proxy and /bucket/{id}
        verify(authorizer, times(2)).authorize(any(AuthorizationRequest.class));
    }

    // Test authorize method for Bucket Authorizable

    @Test
    public void testAuthorizeReadPublicBucketWithNoProxyChain() {
        final Authorizable bucketAuthorizable = authorizableLookup.getBucketAuthorizable(bucketPublic.getIdentifier());
        bucketAuthorizable.authorize(authorizer, RequestAction.READ, USER_NO_PROXY_CHAIN);

        // Should never call authorizer because resource is public
        verify(authorizer, times(0)).authorize(any(AuthorizationRequest.class));
    }

    @Test
    public void testAuthorizeReadPublicBucketWithProxyChain() {
        final Authorizable bucketAuthorizable = authorizableLookup.getBucketAuthorizable(bucketPublic.getIdentifier());
        bucketAuthorizable.authorize(authorizer, RequestAction.READ, USER_WITH_PROXY_CHAIN);

        // Should never call authorizer because resource is public
        verify(authorizer, times(0)).authorize(any(AuthorizationRequest.class));
    }

    @Test
    public void testAuthorizeWritePublicBucketWithUnauthorizedUserAndNoProxyChain() {
        final RequestAction action = RequestAction.WRITE;

        // first request will be to the specific bucket
        final AuthorizationRequest expectedBucketAuthorizationRequest = getBucketAuthorizationRequest(
                bucketPublic.getIdentifier(), action, USER_NO_PROXY_CHAIN);

        when(authorizer.authorize(argThat(new AuthorizationRequestMatcher(expectedBucketAuthorizationRequest))))
                .thenReturn(AuthorizationResult.denied());

        // second request will go to parent of /buckets
        final AuthorizationRequest expectedBucketsAuthorizationRequest = getBucketsAuthorizationRequest(action, USER_NO_PROXY_CHAIN);

        when(authorizer.authorize(argThat(new AuthorizationRequestMatcher(expectedBucketsAuthorizationRequest))))
                .thenReturn(AuthorizationResult.denied());

        // should reach authorizer and throw access denied
        final Authorizable bucketAuthorizable = authorizableLookup.getBucketAuthorizable(bucketPublic.getIdentifier());
        try {
            bucketAuthorizable.authorize(authorizer, action, USER_NO_PROXY_CHAIN);
            Assert.fail("Should have thrown exception");
        } catch (AccessDeniedException e) {
            // Should never call authorizer twice for specific bucket and top-level /buckets
            verify(authorizer, times(2)).authorize(any(AuthorizationRequest.class));
        }
    }

    @Test
    public void testAuthorizeWritePublicBucketWithUnauthorizedProxyChain() {
        final RequestAction action = RequestAction.WRITE;

        // first request will be to authorize the proxy
        final AuthorizationRequest expectedProxyAuthorizationRequest = getProxyAuthorizationRequest(action, USER_WITH_PROXY_CHAIN.getChain());

        when(authorizer.authorize(argThat(new AuthorizationRequestMatcher(expectedProxyAuthorizationRequest))))
                .thenReturn(AuthorizationResult.denied());

        // the authorization of the proxy chain should throw UntrustedProxyException
        final Authorizable bucketAuthorizable = authorizableLookup.getBucketAuthorizable(bucketPublic.getIdentifier());
        try {
            bucketAuthorizable.authorize(authorizer, action, USER_WITH_PROXY_CHAIN);
            Assert.fail("Should have thrown exception");
        } catch (UntrustedProxyException e) {
            // Should call authorizer once for /proxy and then throw exception
            verify(authorizer, times(1)).authorize(any(AuthorizationRequest.class));
        }
    }

    @Test
    public void testAuthorizeWritePublicBucketWithUnauthorizedUserAndAuthorizedProxyChain() {
        final NiFiUser user = USER_WITH_PROXY_CHAIN;
        final RequestAction action = RequestAction.WRITE;

        // first request will be to authorize the proxy
        final AuthorizationRequest expectedProxyAuthorizationRequest = getProxyAuthorizationRequest(action, user.getChain());

        when(authorizer.authorize(argThat(new AuthorizationRequestMatcher(expectedProxyAuthorizationRequest))))
                .thenReturn(AuthorizationResult.approved());

        // second request will be to the specific bucket
        final AuthorizationRequest expectedBucketAuthorizationRequest = getBucketAuthorizationRequest(
                bucketPublic.getIdentifier(), action, user);

        when(authorizer.authorize(argThat(new AuthorizationRequestMatcher(expectedBucketAuthorizationRequest))))
                .thenReturn(AuthorizationResult.denied());

        // third request will go to parent of /buckets
        final AuthorizationRequest expectedBucketsAuthorizationRequest = getBucketsAuthorizationRequest(action, user);

        when(authorizer.authorize(argThat(new AuthorizationRequestMatcher(expectedBucketsAuthorizationRequest))))
                .thenReturn(AuthorizationResult.denied());

        // the authorization of the proxy chain should throw UntrustedProxyException
        final Authorizable bucketAuthorizable = authorizableLookup.getBucketAuthorizable(bucketPublic.getIdentifier());
        try {
            bucketAuthorizable.authorize(authorizer, action, user);
            Assert.fail("Should have thrown exception");
        } catch (AccessDeniedException e) {
            // Should call authorizer three times for /proxy, /bucket/{id}, and /buckets
            verify(authorizer, times(3)).authorize(any(AuthorizationRequest.class));
        }
    }

    @Test
    public void testAuthorizeWritePublicBucketWithAuthorizedUserAndAuthorizedProxyChain() {
        final NiFiUser user = USER_WITH_PROXY_CHAIN;
        final RequestAction action = RequestAction.WRITE;

        // first request will be to authorize the proxy
        final AuthorizationRequest expectedProxyAuthorizationRequest = getProxyAuthorizationRequest(action, user.getChain());

        when(authorizer.authorize(argThat(new AuthorizationRequestMatcher(expectedProxyAuthorizationRequest))))
                .thenReturn(AuthorizationResult.approved());

        // second request will be to the specific bucket
        final AuthorizationRequest expectedBucketAuthorizationRequest = getBucketAuthorizationRequest(
                bucketPublic.getIdentifier(), action, user);

        when(authorizer.authorize(argThat(new AuthorizationRequestMatcher(expectedBucketAuthorizationRequest))))
                .thenReturn(AuthorizationResult.approved());

        // the authorization should all return approved so no exception
        final Authorizable bucketAuthorizable = authorizableLookup.getBucketAuthorizable(bucketPublic.getIdentifier());
        bucketAuthorizable.authorize(authorizer, action, user);

        // Should call authorizer two times for /proxy and /bucket/{id}
        verify(authorizer, times(2)).authorize(any(AuthorizationRequest.class));
    }

    private AuthorizationRequest getBucketAuthorizationRequest(final String bucketIdentifier, final RequestAction action, final NiFiUser user) {
        return new AuthorizationRequest.Builder()
                .resource(ResourceFactory.getBucketResource(bucketIdentifier, bucketIdentifier))
                .action(action)
                .identity(user.getIdentity())
                .accessAttempt(true)
                .anonymous(false)
                .build();
    }

    private AuthorizationRequest getBucketsAuthorizationRequest(final RequestAction action, final NiFiUser user) {
        return new AuthorizationRequest.Builder()
                .resource(ResourceFactory.getBucketsResource())
                .action(action)
                .identity(user.getIdentity())
                .accessAttempt(true)
                .anonymous(false)
                .build();
    }

    private AuthorizationRequest getProxyAuthorizationRequest(final RequestAction action, final NiFiUser user) {
        return new AuthorizationRequest.Builder()
                .resource(ResourceFactory.getProxyResource())
                .action(action)
                .identity(user.getIdentity())
                .accessAttempt(true)
                .anonymous(false)
                .build();
    }

    /**
     * ArugmentMatcher for AuthorizationRequest.
     */
    private static class AuthorizationRequestMatcher implements ArgumentMatcher<AuthorizationRequest> {

        private final AuthorizationRequest expectedAuthorizationRequest;

        public AuthorizationRequestMatcher(final AuthorizationRequest expectedAuthorizationRequest) {
            this.expectedAuthorizationRequest = expectedAuthorizationRequest;
        }

        @Override
        public boolean matches(final AuthorizationRequest authorizationRequest) {
            if (authorizationRequest == null) {
                return false;
            }

            final String requestResourceId = authorizationRequest.getResource().getIdentifier();
            final String expectedResourceId = expectedAuthorizationRequest.getResource().getIdentifier();

            final String requestAction = authorizationRequest.getAction().toString();
            final String expectedAction = expectedAuthorizationRequest.getAction().toString();

            final String requestUserIdentity = authorizationRequest.getIdentity();
            final String expectedUserIdentity = authorizationRequest.getIdentity();

            return requestResourceId.equals(expectedResourceId)
                    && requestAction.equals(expectedAction)
                    && requestUserIdentity.equals(expectedUserIdentity);
        }
    }
}
