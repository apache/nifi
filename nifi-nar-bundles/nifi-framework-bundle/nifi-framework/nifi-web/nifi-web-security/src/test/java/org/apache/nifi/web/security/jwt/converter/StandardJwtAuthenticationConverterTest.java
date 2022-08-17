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
import org.apache.nifi.admin.service.IdpUserGroupService;
import org.apache.nifi.authorization.AccessPolicyProvider;
import org.apache.nifi.authorization.Group;
import org.apache.nifi.authorization.ManagedAuthorizer;
import org.apache.nifi.authorization.UserAndGroups;
import org.apache.nifi.authorization.UserGroupProvider;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.idp.IdpUserGroup;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.web.security.token.NiFiAuthenticationToken;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.security.oauth2.jwt.Jwt;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
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

    @Mock
    private IdpUserGroupService idpUserGroupService;

    private StandardJwtAuthenticationConverter converter;

    @Before
    public void setConverter() {
        final Map<String, String> properties = new HashMap<>();
        final NiFiProperties niFiProperties = NiFiProperties.createBasicNiFiProperties(StringUtils.EMPTY, properties);
        converter = new StandardJwtAuthenticationConverter(authorizer, idpUserGroupService, niFiProperties);

        when(authorizer.getAccessPolicyProvider()).thenReturn(accessPolicyProvider);
        when(accessPolicyProvider.getUserGroupProvider()).thenReturn(userGroupProvider);
        when(userGroupProvider.getUserAndGroups(eq(USERNAME))).thenReturn(userAndGroups);

        final Group group = new Group.Builder().name(AUTHORIZER_GROUP).identifier(AUTHORIZER_GROUP).build();
        when(userAndGroups.getGroups()).thenReturn(Collections.singleton(group));

        final IdpUserGroup idpUserGroup = new IdpUserGroup();
        idpUserGroup.setGroupName(PROVIDER_GROUP);
        when(idpUserGroupService.getUserGroups(eq(USERNAME))).thenReturn(Collections.singletonList(idpUserGroup));
    }

    @Test
    public void testConvert() {
        final JWTClaimsSet claimsSet = new JWTClaimsSet.Builder()
                .subject(USERNAME)
                .build();
        final String token = new PlainJWT(claimsSet).serialize();
        final Jwt jwt = Jwt.withTokenValue(token)
                .header(TYPE_FIELD, JWT_TYPE)
                .subject(USERNAME)
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
