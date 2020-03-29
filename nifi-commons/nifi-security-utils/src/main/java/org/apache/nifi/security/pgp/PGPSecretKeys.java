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
package org.apache.nifi.security.pgp;

import org.bouncycastle.openpgp.PGPSecretKey;

import java.util.HashMap;

public class PGPSecretKeys extends HashMap<String, PGPSecretKey> {
    /**
     * Returns secret key matching the given key id or null.
     *
     * @param keyID secret key id to match
     * @return secret key matching given key ID or null
     */
    public PGPSecretKey getSecretKey(long keyID) {
        for (PGPSecretKey key : this.values()) {
            if (key.getKeyID() == keyID) {
                return key;
            }
        }
        return null;
    }

    /**
     * Returns secret key matching the given user id or null.
     *
     * @param userID secret key user id to match
     * @return secret key matching given user ID or null
     */
    public PGPSecretKey getSecretKey(String userID) {
        return this.get(userID);
    }
}
