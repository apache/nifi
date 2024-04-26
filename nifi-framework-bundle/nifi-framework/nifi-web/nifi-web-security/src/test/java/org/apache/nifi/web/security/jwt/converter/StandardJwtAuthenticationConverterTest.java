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

import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.PlainJWT;
import org.apache.nifi.authorization.AccessPolicyProvider;
import org.apache.nifi.authorization.Group;
import org.apache.nifi.authorization.ManagedAuthorizer;
import org.apache.nifi.authorization.UserAndGroups;
import org.apache.nifi.authorization.UserGroupProvider;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.web.security.jwt.provider.SupportedClaim;
import org.apache.nifi.web.security.token.NiFiAuthenticationToken;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.oauth2.jwt.Jwt;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class StandardJwtAuthenticationConverterTest {
    private static final String USERNAME = "NiFi";

    private static final String AUTHORIZER_GROUP = "AuthorizerGroup";

    private static final String PROVIDER_GROUP = "ProviderGroup";

    private static final String TYPE_FIELD = "typ";

    private static final String JWT_TYPE = "JWT";

    @Mock
    private ManagedAuthorizer authorizer;

    @Mock
    private AccessPolicyProvider accessPolicyProvider;

    @Mock
    private UserGroupProvider userGroupProvider;

    @Mock
    private UserAndGroups userAndGroups;

    private StandardJwtAuthenticationConverter converter;

    @BeforeEach
    public void setConverter() {
        final Map<String, String> properties = new HashMap<>();
        final NiFiProperties niFiProperties = NiFiProperties.createBasicNiFiProperties(StringUtils.EMPTY, properties);
        converter = new StandardJwtAuthenticationConverter(authorizer, niFiProperties);

        when(authorizer.getAccessPolicyProvider()).thenReturn(accessPolicyProvider);
        when(accessPolicyProvider.getUserGroupProvider()).thenReturn(userGroupProvider);
        when(userGroupProvider.getUserAndGroups(eq(USERNAME))).thenReturn(userAndGroups);

        final Group group = new Group.Builder().name(AUTHORIZER_GROUP).identifier(AUTHORIZER_GROUP).build();
        when(userAndGroups.getGroups()).thenReturn(Collections.singleton(group));
    }

    @Test
    public void testConvert() {
        final List<String> providerGroups = Collections.singletonList(PROVIDER_GROUP);

        final JWTClaimsSet claimsSet = new JWTClaimsSet.Builder()
                .subject(USERNAME)
                .claim(SupportedClaim.GROUPS.getClaim(), providerGroups)
                .build();
        final String token = new PlainJWT(claimsSet).serialize();
        final Jwt jwt = Jwt.withTokenValue(token)
                .header(TYPE_FIELD, JWT_TYPE)
                .subject(USERNAME)
                .claim(SupportedClaim.GROUPS.getClaim(), providerGroups)
                .build();

        final NiFiAuthenticationToken authenticationToken = converter.convert(jwt);
        assertNotNull(authenticationToken);
        assertEquals(USERNAME, authenticationToken.toString());

        final NiFiUserDetails details = (NiFiUserDetails) authenticationToken.getDetails();
        final NiFiUser user = details.getNiFiUser();

        final Set<String> expectedGroups = Collections.singleton(AUTHORIZER_GROUP);
        assertEquals(expectedGroups, user.getGroups());

        final Set<String> expectedProviderGroups = Collections.singleton(PROVIDER_GROUP);
        assertEquals(expectedProviderGroups, user.getIdentityProviderGroups());
    }
}
