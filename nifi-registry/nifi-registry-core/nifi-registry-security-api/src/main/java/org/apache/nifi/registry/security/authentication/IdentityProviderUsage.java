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

public interface IdentityProviderUsage {

    /**
     * Provides the usage instructions for an identity provider.
     *
     * The instructions should target a human consumer of the
     * NiFi Registry REST API that needs to know how to handle
     * Authentication when using / programming an API client.
     *
     * @return the usage instructions for an identity provider
     */
    String getText();

    /**
     * If the identity provider follows an HTTP standard auth
     * scheme, this provides which scheme is being used
     * (or "Other" if the identity provider follows its own scheme).
     *
     * In the case the scheme is well understood, such as HTTP
     * "Basic" Auth, this may be sufficient. In other cases,
     * {@link #getText()} should provider detailed human-readable
     * instructions about how a client should interact with
     * the {@link IdentityProvider}.
     *
     * @return an enum for the auth
     */
    AuthType getAuthType();

    /**
     * Standard auth types as maintained by IANA:
     * https://www.iana.org/assignments/http-authschemes/http-authschemes.xhtml
     *
     * Note, draft and experimental standards are not included, nor are app-specific custom schemes.
     * To create an enum for such a scheme, use OTHER with a custom httpAuthScheme string, e.g.:
     *
     * <code>AuthType myAuthType = AuthType.OTHER.httpAuthScheme("my-auth-scheme");</code>
     */
    enum AuthType {

        /**
         * Indicates the AuthType is unknown. Can be used in places where an AuthType is required but unknown by default.
         */
        UNKNOWN(0, "Unknown"),

        /**
         * HTTP Basic Auth as defined by RFC7617.
         */
        BASIC(1, "Basic"),

        /**
         * HTTP Bearer Auth as defined by RFC6750.
         */
        BEARER(2, "Bearer"),

        /**
         * HTTP Digest Auth as defined by RFC7616.
         */
        DIGEST(3, "Digest"),

        /**
         * HTTP Negotiate (SPNEGO) Auth as defined by RFC4559.
         */
        NEGOTIATE(4, "Negotiate"),

        /**
         * HTTP OAuth as defined by RFC5849
         */
        OAUTH(5, "OAuth"),

        /**
         * A distinct AuthType for which there is not yet a defined enumeration value.
         * If a HTTP Auth Scheme should be set (e.g., for use in a WWW-Authenticate challenge list)
         * use the setter, i.e.:
         * <code>AuthType myAuthType = AuthType.OTHER.httpAuthScheme("my-auth-scheme");</code>
         */
        OTHER(99, "Other"),
        ;

        private final int code;
        private String httpAuthScheme;

        private AuthType(int statusCode, String httpAuthScheme) {
            this.code = statusCode;
            this.httpAuthScheme = httpAuthScheme;
        }

        public int getStatusCode() {
            return this.code;
        }

        public String getHttpAuthScheme() {
            return this.toString();
        }

        public AuthType httpAuthScheme(String httpAuthScheme) {
            if (httpAuthScheme != null) {
                this.httpAuthScheme = httpAuthScheme;
            }
            return this;
        }

        public String toString() {
            return this.httpAuthScheme;
        }

        public static AuthType fromCode(int code) {
            AuthType[] enumTypes = values();
            for (AuthType s : enumTypes) {
                if (s.code == code) {
                    return s;
                }
            }
            return null;
        }
    }

}
