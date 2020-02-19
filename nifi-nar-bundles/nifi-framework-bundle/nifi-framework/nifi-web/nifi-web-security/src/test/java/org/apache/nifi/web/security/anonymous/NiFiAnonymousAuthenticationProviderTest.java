package org.apache.nifi.web.security.anonymous;

import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.web.security.InvalidAuthenticationException;
import org.apache.nifi.web.security.token.NiFiAuthenticationToken;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NiFiAnonymousAuthenticationProviderTest {

    private static final Logger logger = LoggerFactory.getLogger(NiFiAnonymousAuthenticationProviderTest.class);

    @Test
    public void testAnonymousDisabledNotSecure() throws Exception {
        final NiFiProperties nifiProperties = Mockito.mock(NiFiProperties.class);
        when(nifiProperties.isAnonymousAuthenticationAllowed()).thenReturn(false);

        final NiFiAnonymousAuthenticationProvider anonymousAuthenticationProvider = new NiFiAnonymousAuthenticationProvider(nifiProperties, mock(Authorizer.class));

        final NiFiAnonymousAuthenticationRequestToken authenticationRequest = new NiFiAnonymousAuthenticationRequestToken(false, StringUtils.EMPTY);

        final NiFiAuthenticationToken authentication = (NiFiAuthenticationToken) anonymousAuthenticationProvider.authenticate(authenticationRequest);
        final NiFiUserDetails userDetails = (NiFiUserDetails) authentication.getDetails();
        assertTrue(userDetails.getNiFiUser().isAnonymous());
    }

    @Test
    public void testAnonymousEnabledNotSecure() throws Exception {
        final NiFiProperties nifiProperties = Mockito.mock(NiFiProperties.class);
        when(nifiProperties.isAnonymousAuthenticationAllowed()).thenReturn(true);

        final NiFiAnonymousAuthenticationProvider anonymousAuthenticationProvider = new NiFiAnonymousAuthenticationProvider(nifiProperties, mock(Authorizer.class));

        final NiFiAnonymousAuthenticationRequestToken authenticationRequest = new NiFiAnonymousAuthenticationRequestToken(false, StringUtils.EMPTY);

        final NiFiAuthenticationToken authentication = (NiFiAuthenticationToken) anonymousAuthenticationProvider.authenticate(authenticationRequest);
        final NiFiUserDetails userDetails = (NiFiUserDetails) authentication.getDetails();
        assertTrue(userDetails.getNiFiUser().isAnonymous());
    }

    @Test(expected = InvalidAuthenticationException.class)
    public void testAnonymousDisabledSecure() throws Exception {
        final NiFiProperties nifiProperties = Mockito.mock(NiFiProperties.class);
        when(nifiProperties.isAnonymousAuthenticationAllowed()).thenReturn(false);

        final NiFiAnonymousAuthenticationProvider anonymousAuthenticationProvider = new NiFiAnonymousAuthenticationProvider(nifiProperties, mock(Authorizer.class));

        final NiFiAnonymousAuthenticationRequestToken authenticationRequest = new NiFiAnonymousAuthenticationRequestToken(true, StringUtils.EMPTY);

        anonymousAuthenticationProvider.authenticate(authenticationRequest);
    }

    @Test
    public void testAnonymousEnabledSecure() throws Exception {
        final NiFiProperties nifiProperties = Mockito.mock(NiFiProperties.class);
        when(nifiProperties.isAnonymousAuthenticationAllowed()).thenReturn(true);

        final NiFiAnonymousAuthenticationProvider anonymousAuthenticationProvider = new NiFiAnonymousAuthenticationProvider(nifiProperties, mock(Authorizer.class));

        final NiFiAnonymousAuthenticationRequestToken authenticationRequest = new NiFiAnonymousAuthenticationRequestToken(true, StringUtils.EMPTY);

        final NiFiAuthenticationToken authentication = (NiFiAuthenticationToken) anonymousAuthenticationProvider.authenticate(authenticationRequest);
        final NiFiUserDetails userDetails = (NiFiUserDetails) authentication.getDetails();
        assertTrue(userDetails.getNiFiUser().isAnonymous());
    }
}
