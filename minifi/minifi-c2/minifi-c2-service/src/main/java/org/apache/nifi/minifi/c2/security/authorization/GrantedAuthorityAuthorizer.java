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

package org.apache.nifi.minifi.c2.security.authorization;

import org.apache.nifi.minifi.c2.api.security.authorization.AuthorizationException;
import org.apache.nifi.minifi.c2.api.security.authorization.Authorizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.yaml.snakeyaml.Yaml;

import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class GrantedAuthorityAuthorizer implements Authorizer {
    private static final Logger logger = LoggerFactory.getLogger(GrantedAuthorityAuthorizer.class);

    public static final String DENY = "deny";
    public static final String ALLOW = "allow";
    public static final String DEFAULT_ACTION = "Default Action";
    private final Map<String, Object> grantedAuthorityMap;

    public GrantedAuthorityAuthorizer(Resource configYaml) throws IOException {
        try (InputStream inputStream = configYaml.getInputStream()) {
            grantedAuthorityMap = as(Map.class, new Yaml().load(inputStream), o -> new IllegalArgumentException("Expected yaml map for root of configuration but was " + o));
        }
    }

    @Override
    public void authorize(Authentication authentication, UriInfo uriInfo) throws AuthorizationException {
        if (authentication == null) {
            throw new AuthorizationException("null authentication object provided.");
        }

        if (!authentication.isAuthenticated()) {
            throw new AuthorizationException(authentication + " not authenticated.");
        }

        Set<String> authorities = authentication.getAuthorities().stream().map(GrantedAuthority::getAuthority).collect(Collectors.toSet());

        String defaultAction = as(String.class, grantedAuthorityMap.getOrDefault(DEFAULT_ACTION, DENY));
        String path = uriInfo.getAbsolutePath().getPath();
        Map<String, Object> pathAuthorizations = as(Map.class, grantedAuthorityMap.get("Paths"));
        if (pathAuthorizations == null && !ALLOW.equalsIgnoreCase(defaultAction)) {
            throw new AuthorizationException("Didn't find authorizations for " + path + " and default policy is " + defaultAction + " instead of allow");
        }

        Map<String, Object> pathAuthorization = as(Map.class, pathAuthorizations.get(path));
        if (pathAuthorization == null && !ALLOW.equalsIgnoreCase(defaultAction)) {
            throw new AuthorizationException("Didn't find authorizations for " + path + " and default policy is " + defaultAction + " instead of allow");
        }
        defaultAction = as(String.class, pathAuthorization.getOrDefault(DEFAULT_ACTION, defaultAction));
        List<Map<String, Object>> actions = as(List.class, pathAuthorization.get("Actions"));
        MultivaluedMap<String, String> queryParameters = uriInfo.getQueryParameters();
        for (Map<String, Object> action : actions) {
            String ruleAction = as(String.class, action.get("Action"));
            if (ruleAction == null || !(ALLOW.equalsIgnoreCase(ruleAction) || DENY.equalsIgnoreCase(ruleAction))) {
                throw new AuthorizationException("Expected Action key of allow or deny for " + action);
            }
            String authorization = as(String.class, action.get("Authorization"));
            if (authorization != null && !authorities.contains(authorization)) {
                continue;
            }
            Map<String, Object> parameters = as(Map.class, action.get("Query Parameters"));
            if (parameters != null) {
                boolean foundParameterMismatch = false;
                for (Map.Entry<String, Object> parameter : parameters.entrySet()) {
                    Object value = parameter.getValue();
                    if (value instanceof String) {
                        value = Arrays.asList((String)value);
                    }
                    if (!Objects.equals(queryParameters.get(parameter.getKey()), value)) {
                        foundParameterMismatch = true;
                        break;
                    }
                }
                if (foundParameterMismatch) {
                    continue;
                }
            }
            if (ALLOW.equalsIgnoreCase(ruleAction)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Action " + action + "matched which resulted in " + ruleAction);
                }
                return;
            } else {
                throw new AuthorizationException("Action " + action + " matched which resulted in " + ruleAction);
            }
        }
        if (ALLOW.equalsIgnoreCase(defaultAction)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Found no matching actions so falling back to default action " + defaultAction);
            }
        } else {
            throw new AuthorizationException("Didn't find authorizations for " + path + " and default policy is " + defaultAction + " instead of allow");
        }
    }

    private static <T> T as(Class<T> clazz, Object object) throws AuthorizationException {
        return as(clazz, object, o -> new AuthorizationException("Expected " + clazz + " but was " + o));
    }

    private static <T, E extends Throwable> T as(Class<T> clazz, Object object, Function<Object, E> exceptionSupplier) throws E {
        if (object == null) {
            return null;
        }
        if (!clazz.isInstance(object)) {
            throw exceptionSupplier.apply(object);
        }
        return clazz.cast(object);
    }
}
