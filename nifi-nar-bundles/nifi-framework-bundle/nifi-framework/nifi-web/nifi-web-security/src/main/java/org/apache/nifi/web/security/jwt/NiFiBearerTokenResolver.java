package org.apache.nifi.web.security.jwt;

import org.apache.nifi.web.security.InvalidAuthenticationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NiFiBearerTokenResolver implements BearerTokenResolver {
    private static final Logger logger = LoggerFactory.getLogger(NiFiBearerTokenResolver.class);
    private static final Pattern BEARER_HEADER_PATTERN = Pattern.compile("^Bearer (\\S*\\.\\S*\\.\\S*)$");
    private static final Pattern JWT_PATTERN = Pattern.compile("^(\\S*\\.\\S*\\.\\S*)$");
    public static final String AUTHORIZATION = "Authorization";

    @Override
    public String resolve(HttpServletRequest request) {
        final String authorizationHeader = request.getHeader(AUTHORIZATION);

        // if there is no authorization header, we don't know the user
        if (authorizationHeader == null || !validAuthorizationHeaderFormat(authorizationHeader)) {
            logger.debug("Authorization header was not present or not in a valid format.");
            return null;
        } else {
            // Extract the Base64 encoded token from the Authorization header
            return getTokenFromHeader(authorizationHeader);
        }
    }

    public boolean validAuthorizationHeaderFormat(String authorizationHeader) {
        Matcher matcher = BEARER_HEADER_PATTERN.matcher(authorizationHeader);
        return matcher.matches();
    }

    public boolean validJwtFormat(String jwt) {
        Matcher matcher = JWT_PATTERN.matcher(jwt);
        return matcher.matches();
    }

    private String getTokenFromHeader(String authenticationHeader) {
        Matcher matcher = BEARER_HEADER_PATTERN.matcher(authenticationHeader);
        if(matcher.matches()) {
            return matcher.group(1);
        } else {
            throw new InvalidAuthenticationException("JWT did not match expected pattern.");
        }
    }
}
