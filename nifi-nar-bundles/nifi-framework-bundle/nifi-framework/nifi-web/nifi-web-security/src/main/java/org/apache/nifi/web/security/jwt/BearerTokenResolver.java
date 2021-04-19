package org.apache.nifi.web.security.jwt;

import javax.servlet.http.HttpServletRequest;

public interface BearerTokenResolver {
    /**
     * Resolve any
     * <a href="https://tools.ietf.org/html/rfc6750#section-1.2" target="_blank">Bearer
     * Token</a> value from the request.
     * @param request the request
     * @return the Bearer Token value or {@code null} if none found
     */
    String resolve(HttpServletRequest request);
}
