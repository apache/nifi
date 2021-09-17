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
package org.apache.nifi.web.security.jwt.key.command;

import org.apache.nifi.web.security.jwt.key.service.VerificationKeyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Key Expiration Command removes expired Verification Keys
 */
public class KeyExpirationCommand implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(KeyExpirationCommand.class);

    private final VerificationKeyService verificationKeyService;

    public KeyExpirationCommand(final VerificationKeyService verificationKeyService) {
        this.verificationKeyService = Objects.requireNonNull(verificationKeyService, "Verification Key Service required");
    }

    /**
     * Run deletes expired Verification Keys
     */
    @Override
    public void run() {
        LOGGER.debug("Delete Expired Verification Keys Started");
        verificationKeyService.deleteExpired();
    }
}
