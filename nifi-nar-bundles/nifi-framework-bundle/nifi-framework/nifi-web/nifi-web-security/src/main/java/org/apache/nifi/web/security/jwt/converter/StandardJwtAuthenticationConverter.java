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
package org.apache.nifi.web.security.jwt.converter;

import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.authorization.user.StandardNiFiUser;
import org.apache.nifi.authorization.util.IdentityMapping;
import org.apache.nifi.authorization.util.IdentityMappingUtil;
import org.apache.nifi.authorization.util.UserGroupUtil;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.jwt.provider.SupportedClaim;
import org.apache.nifi.web.security.token.NiFiAuthenticationToken;
import org.springframework.core.convert.converter.Converter;
import org.springframework.security.oauth2.jwt.Jwt;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Standard Converter from JSON Web Token to NiFi Authentication Token
 */
public class StandardJwtAuthenticationConverter implements Converter<Jwt, NiFiAuthenticationToken> {
    private final Authorizer authorizer;

    private final List<IdentityMapping> identityMappings;

    public StandardJwtAuthenticationConverter(final Authorizer authorizer, final NiFiProperties properties) {
        this.authorizer = authorizer;
        this.identityMappings = IdentityMappingUtil.getIdentityMappings(properties);
    }

    /**
     * Convert JSON Web Token to NiFi Authentication Token
     *
     * @param jwt JSON Web Token
     * @return NiFi Authentication Token
     */
    @Override
    public NiFiAuthenticationToken convert(final Jwt jwt) {
        final NiFiUser user = getUser(jwt);
        return new NiFiAuthenticationToken(new NiFiUserDetails(user));
    }

    private NiFiUser getUser(final Jwt jwt) {
        final String identity = IdentityMappingUtil.mapIdentity(jwt.getSubject(), identityMappings);

        final Set<String> providedGroups = getProvidedGroups(jwt);
        return new StandardNiFiUser.Builder()
                .identity(identity)
                .groups(UserGroupUtil.getUserGroups(authorizer, identity))
                .identityProviderGroups(providedGroups)
                .build();
    }

    private Set<String> getProvidedGroups(final Jwt jwt) {
        final List<String> claimGroups = jwt.getClaimAsStringList(SupportedClaim.GROUPS.getClaim());

        final Set<String> providedGroups;
        if (claimGroups == null) {
            providedGroups = Collections.emptySet();
        } else {
            providedGroups = new LinkedHashSet<>(claimGroups);
        }
        return providedGroups;
    }
}
