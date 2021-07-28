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
package org.apache.nifi.web.security.jwt.revocation.command;

import org.apache.nifi.web.security.jwt.revocation.JwtRevocationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Revocation Expiration Command removes expired Revocations
 */
public class RevocationExpirationCommand implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RevocationExpirationCommand.class);

    private final JwtRevocationService jwtRevocationService;

    public RevocationExpirationCommand(final JwtRevocationService jwtRevocationService) {
        this.jwtRevocationService = Objects.requireNonNull(jwtRevocationService, "JWT Revocation Service required");
    }

    /**
     * Run deletes expired Revocations
     */
    @Override
    public void run() {
        LOGGER.debug("Delete Expired Revocations Started");
        jwtRevocationService.deleteExpired();
    }
}
