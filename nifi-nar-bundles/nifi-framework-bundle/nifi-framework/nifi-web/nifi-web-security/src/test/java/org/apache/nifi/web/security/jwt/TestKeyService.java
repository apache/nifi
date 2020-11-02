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

import java.util.ArrayList;
import java.util.UUID;
import org.apache.nifi.admin.service.KeyService;
import org.apache.nifi.key.Key;

public class TestKeyService implements KeyService {

    ArrayList<Key> signingKeys = new ArrayList<Key>();

    public TestKeyService() {

    }

    @Override
    public Key getKey(int id) {
        Key key = null;
        for(Key k : signingKeys) {
            if(k.getId() == id) {
                key = k;
            }
        }
        return key;
    }

    @Override
    public Key getOrCreateKey(String identity) {
        for(Key key : signingKeys) {
            if(key.getIdentity().equals(identity)) {
                return key;
            }
        }

        Key key = generateKey(identity);
        signingKeys.add(key);
        return key;
    }

    @Override
    public void deleteKey(Integer keyId) {
        Key keyToRemove = null;
        for(Key k : signingKeys) {
            if(k.getId() == keyId) {
                keyToRemove = k;
            }
        }
        signingKeys.remove(keyToRemove);
    }

    private Key generateKey(String identity) {
        Integer keyId = signingKeys.size();
        return new Key(keyId, identity, UUID.randomUUID().toString());
    }
}
