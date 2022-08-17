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

/**
 * Password Encoder for encoding and matching passwords modeled after Spring Security PasswordEncoder
 */
public interface PasswordEncoder {
    /**
     * Encode Password and return hashed password
     *
     * @param password Password
     * @return Encoded Password
     */
    String encode(char[] password);

    /**
     * Match Password against encoded password
     *
     * @param password Password to be matched
     * @param encodedPassword Encoded representation of password for matching
     * @return Matched status
     */
    boolean matches(char[] password, String encodedPassword);
}
