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
package org.apache.nifi.pgp.service.api;

import org.apache.nifi.controller.ControllerService;
import org.bouncycastle.openpgp.PGPPrivateKey;

import java.util.Optional;

/**
 * Pretty Good Privacy Private Key Service abstracts access to Private Keys used for decryption
 */
public interface PGPPrivateKeyService extends ControllerService {
    /**
     * Find Private Key
     *
     * @param keyIdentifier Private Key Identifier
     * @return Optional container for PGP Private Key
     */
    Optional<PGPPrivateKey> findPrivateKey(long keyIdentifier);
}
