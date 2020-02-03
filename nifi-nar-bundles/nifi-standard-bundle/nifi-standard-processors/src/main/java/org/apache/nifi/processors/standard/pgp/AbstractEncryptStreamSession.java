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
package org.apache.nifi.processors.standard.pgp;

import org.apache.nifi.logging.ComponentLog;
import org.bouncycastle.openpgp.PGPEncryptedDataGenerator;

import java.security.SecureRandom;

/**
 * This is a shared base class for other encrypt stream session classes, e.g., pub key and pbe.
 *
 */
class AbstractEncryptStreamSession implements EncryptStreamSession {
    static final SecureRandom random = new SecureRandom();
    PGPEncryptedDataGenerator generator;
    private ComponentLog logger;
    private boolean armor;

    AbstractEncryptStreamSession(ComponentLog logger, boolean armor) {
        this.logger = logger;
        this.armor = armor;
    }

    /**
     * Returns the encrypted data generator.
     *
     * @return the encrypted data generator
     */
    public PGPEncryptedDataGenerator getDataGenerator() {
        return generator;
    }

    /**
     * Returns the armor value.
     *
     * @return the armor value
     */
    public boolean getArmor() {
        return armor;
    }

    /**
     * Returns the logger.
     *
     * @return the logger
     */
    public ComponentLog getLogger() {
        return logger;
    }
}
