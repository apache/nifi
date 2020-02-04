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
import org.bouncycastle.openpgp.PGPEncryptedData;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPPrivateKey;
import org.bouncycastle.openpgp.PGPPublicKeyEncryptedData;
import org.bouncycastle.openpgp.operator.PublicKeyDataDecryptorFactory;
import org.bouncycastle.openpgp.operator.bc.BcPublicKeyDataDecryptorFactory;

import java.io.InputStream;

/**
 * This class encapsulates a decryption session with a private key.
 *
 */
class PrivateKeyDecryptStreamSession implements DecryptStreamSession {
    private final PublicKeyDataDecryptorFactory pkFactory;
    final ComponentLog logger;

    PrivateKeyDecryptStreamSession(ComponentLog logger, PGPPrivateKey privateKey) {
        this.logger = logger;
        pkFactory = new BcPublicKeyDataDecryptorFactory(privateKey);
    }

    public InputStream getInputStream(PGPEncryptedData packet) throws PGPException {
        return ((PGPPublicKeyEncryptedData) packet).getDataStream(pkFactory);
    }

    @Override
    public ComponentLog getLogger() {
        return logger;
    }

}
