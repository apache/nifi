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
package org.apache.nifi.registry.security.authentication;

import org.apache.nifi.registry.security.authentication.exception.IdentityAccessException;
import org.apache.nifi.registry.security.authentication.exception.InvalidCredentialsException;
import org.apache.nifi.registry.security.exception.SecurityProviderCreationException;
import org.apache.nifi.registry.security.exception.SecurityProviderDestructionException;

import javax.servlet.http.HttpServletRequest;

/**
 * IdentityProvider is an interface for a class that is able to establish a client identity.
 *
 * Specifically, this provider can:
 *  - extract credentials from an HttpServletRequest (eg, parse a header, form parameter, or client certificates)
 *  - authenticate those credentials and map them to an authenticated identity value
 *    (eg, determine a username given a valid auth token)
 */
public interface IdentityProvider {

    /**
     * @return an IdentityProviderUsage that describes the expectations of the inputs
     *         to {@link #authenticate(AuthenticationRequest)}
     */
    IdentityProviderUsage getUsageInstructions();

    /**
     * Extracts credentials from an {@link HttpServletRequest}.
     *
     * First, a check to the HttpServletRequest should be made to determine if this IdentityProvider is
     * well suited to authenticate the request. For example, if the IdentityProvider is designed to read
     * a particular header field to look for a token or identity claim, the check might be that the proper
     * header field exists and (if a shared header field, such as "Authorization") that the format of the
     * value in the header matches the expected format for this identity provider (e.g., must start with
     * a prefix such as "Bearer"). Note, the expectations of the HttpServletRequest can be described by
     * the {@link #getUsageInstructions()} method.
     *
     * If this check fails, this method should return null. This will indicate to the framework that the
     * IdentityProvider does not recognize an identity claim present in the HttpServletRequest and that
     * the framework should try another IdentityProvider.
     *
     * If the identity claim format is recognized, it should be extracted and returned in an
     * {@link AuthenticationRequest}. The types and values set in the {@link AuthenticationRequest} are
     * left to the discretion of the IdentityProvider, as the intended audience of the request is the
     * {@link #authenticate(AuthenticationRequest)} method, where the corresponding logic to interpret
     * an {@link AuthenticationRequest} can be implemented. As a rule of thumb, any values that could be considered
     * sensitive, such as a password or persistent token susceptible to replay attacks, should be stored
     * in the credentials field of the {@link AuthenticationRequest} as the framework will make the most effort
     * to protect that value, including obscuring it in toString() output.
     *
     * If the {@link AuthenticationRequest} is insufficient or too generic for this IdentityProvider implementation,
     * this IdentityProvider may subclass {@link AuthenticationRequest} to create a credentials-bearing request
     * object that is better suited for this IdentityProvider implementation. In that case, the implementation
     * might wish to also override the {@link #supports(Class)} method to indicate what types of request
     * objects it supports in the call to {@link #authenticate(AuthenticationRequest)}.
     *
     * If credential location is recognized in the {@link HttpServletRequest} but extraction fails,
     * in most cases that exceptional case should be caught, logged, and null should be returned, as it
     * is possible another IdentityProvider will be able to parse the credentials or find a separate
     * set of credentials in the {@link HttpServletRequest} (e.g., a request containing an Authorization
     * header and a client certificate.)
     *
     * @param servletRequest the {@link HttpServletRequest} request that may contain credentials
     *                       understood by this IdentityProvider
     * @return an AuthenticationRequest containing the extracted credentials in a format this
     *         IdentityProvider understands, or null if no credentials could be found in or extracted
     *         successfully from the servletRequest
     */
    AuthenticationRequest extractCredentials(HttpServletRequest servletRequest);

    /**
     * Authenticates the credentials passed in the {@link AuthenticationRequest}.
     *
     * In typical usage, the AuthenticationRequest argument is expected to originate from this
     * IdentityProvider's {@link #extractCredentials} method, so the logic for interpreting the
     * values in the {@link AuthenticationRequest} should correspond to how the {@link AuthenticationRequest}
     * is formed there.
     *
     * The first step of authentication should be to check if the credentials are understandable
     * by this IdentityProvider. If this check fails, this method should return null. This will
     * indicate to the framework that the IdentityProvider is not able to make a judgement call
     * on if the request can be authenticated, and the framework can check with another IdentityProvider
     * if one is available.
     *
     * If this IdentityProvider is able to interpret the AuthenticationRequest, it should perform
     * and authentication check. If the authentication check fails, an exception should be thrown.
     * Use an {@link InvalidCredentialsException} if the authentication check completed and the
     * credentials failed authentication. Use an {@link IdentityAccessException} if a dependency
     * service or provider fails, such as an failure to read a persistent store of identity or
     * credential data. Either exception type will indicate to the framework that this IdentityProvider's
     * opinion is that the client making the request should be blocked from accessing a resource
     * that requires authentication. (Versus a null return value, which is an indication that this
     * IdentityProvider is not well suited to make a judgement call one way or the other.)
     *
     * @param authenticationRequest the request, containing identity claim credentials for the
     *                              IdentityProvider to authenticate and determine an identity
     * @return The authentication response containing a fully populated identity value,
     *         or null if identity cannot be determined
     * @throws InvalidCredentialsException The login credentials were interpretable by this
     *                                     IdentityProvider and failed authentication
     * @throws IdentityAccessException Unable to assign an identity due to an issue accessing
     *                                 underlying storage or service
     */
    AuthenticationResponse authenticate(AuthenticationRequest authenticationRequest)
            throws InvalidCredentialsException, IdentityAccessException;

    /**
     * Allows this IdentityProvider to declare support for specific subclasses of {@link AuthenticationRequest}.
     *
     * In normal usage, only an AuthenticationRequest originating from this IdentityProvider's
     * {@link #extractCredentials(HttpServletRequest)} method will be passed to {@link #authenticate(AuthenticationRequest)}.
     * However, when IdentityProviders are used with another framework,
     * another component may formulate the AuthenticationRequest to pass to the
     * {@link #authenticate(AuthenticationRequest)} method. This allows a caller to
     * check if the IdentityProvider can support the AuthenticationRequest class.
     * If the caller knows the IdentityProvider can support the AuthenticationRequest
     * (e.g., it was generated by calling {@link #extractCredentials(HttpServletRequest)},
     * this check is optional and does not need to be performed.
     *
     * @param authenticationRequestClazz the class the caller wants to check
     * @return a boolean value indicating if this IdentityProvider supports authenticationRequestClazz
     */
    default boolean supports(Class<? extends AuthenticationRequest> authenticationRequestClazz) {
        return AuthenticationRequest.class.equals(authenticationRequestClazz);
    }

    /**
     * Called to configure the AuthorityProvider after instance creation.
     *
     * @param configurationContext at the time of configuration
     * @throws SecurityProviderCreationException for any issues configuring the provider
     */
    void onConfigured(IdentityProviderConfigurationContext configurationContext) throws SecurityProviderCreationException;

    /**
     * Called immediately before instance destruction for implementers to release resources.
     *
     * @throws SecurityProviderDestructionException If pre-destruction fails.
     */
    void preDestruction() throws SecurityProviderDestructionException;

}
