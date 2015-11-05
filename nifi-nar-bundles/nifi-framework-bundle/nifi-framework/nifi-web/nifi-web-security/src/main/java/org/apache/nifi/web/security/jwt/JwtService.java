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
package org.apache.nifi.web.security.jwt;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.MalformedJwtException;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.SignatureException;
import io.jsonwebtoken.UnsupportedJwtException;
import io.jsonwebtoken.impl.TextCodec;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Calendar;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.util.NiFiProperties;
import org.springframework.security.core.Authentication;

/**
 *
 */
public class JwtService {

    private final static String AUTHORIZATION = "Authorization";

    private final String key;
    private final Integer expires;

    public JwtService(final NiFiProperties properties) {
        // TODO - load key (and algo/provider?) and expiration from properties

        key = TextCodec.BASE64.encode("nififtw!");
        expires = 1;
    }

    /**
     * Gets the Authentication by extracting a JWT token from the specified request.
     *
     * @param request Request to extract the token from
     * @return The user identifier from the token
     */
    public String getAuthentication(final HttpServletRequest request) {
        // extract/verify token from incoming request
        final String authorization = request.getHeader(AUTHORIZATION);
        final String token = StringUtils.substringAfterLast(authorization, " ");

        try {
            final Jws<Claims> jwt = Jwts.parser().setSigningKey(key).parseClaimsJws(token);
            return jwt.getBody().getSubject();
        } catch (final MalformedJwtException | UnsupportedJwtException | SignatureException | ExpiredJwtException | IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * Adds a token for the specified authentication in the specified response.
     *
     * @param response The response to add the token to
     * @param authentication The authentication to generate a token for
     * @throws java.io.IOException if an io exception occurs
     */
    public void addToken(final HttpServletResponse response, final Authentication authentication) throws IOException {
        // set expiration to one day from now
        final Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, expires);

        // create a token the specified authentication
        final String identity = authentication.getPrincipal().toString();
        final String username = authentication.getName();
        final String token = Jwts.builder().setSubject(identity).claim("preferred_username", username).setExpiration(calendar.getTime()).signWith(SignatureAlgorithm.HS512, key).compact();

        // add the token as a response header
        final PrintWriter out = response.getWriter();
        out.print(token);

        // mark the response as successful
        response.setStatus(HttpServletResponse.SC_CREATED);
        response.setContentType("text/plain");
    }

}
