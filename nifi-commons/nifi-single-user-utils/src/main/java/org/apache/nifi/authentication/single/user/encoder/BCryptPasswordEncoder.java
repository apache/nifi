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
package org.apache.nifi.authentication.single.user.encoder;

import at.favre.lib.crypto.bcrypt.BCrypt;

/**
 * Password Encoder implementation using bcrypt hashing
 */
public class BCryptPasswordEncoder implements PasswordEncoder {
    private static final int DEFAULT_COST = 12;

    private static final BCrypt.Version BCRYPT_VERSION = BCrypt.Version.VERSION_2B;

    private static final BCrypt.Hasher HASHER = BCrypt.with(BCRYPT_VERSION);

    private static final BCrypt.Verifyer VERIFYER = BCrypt.verifyer(BCRYPT_VERSION);

    /**
     * Encode Password and return bcrypt hashed password
     *
     * @param password Password
     * @return bcrypt hashed password
     */
    @Override
    public String encode(final char[] password) {
        return HASHER.hashToString(DEFAULT_COST, password);
    }

    /**
     * Match Password against bcrypt hashed password
     *
     * @param password Password to be matched
     * @param encodedPassword bcrypt hashed password
     * @return Matched status
     */
    @Override
    public boolean matches(final char[] password, final String encodedPassword) {
        final BCrypt.Result result = VERIFYER.verifyStrict(password, encodedPassword.toCharArray());
        return result.verified;
    }
}
