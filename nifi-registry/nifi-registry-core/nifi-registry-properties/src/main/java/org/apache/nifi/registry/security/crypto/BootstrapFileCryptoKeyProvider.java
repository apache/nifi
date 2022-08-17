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
package org.apache.nifi.registry.security.crypto;

import org.apache.nifi.registry.properties.util.NiFiRegistryBootstrapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * An implementation of {@link CryptoKeyProvider} that loads the key from disk every time it is needed.
 *
 * The persistence-backing of the key is in the bootstrap.conf file, which must be provided to the
 * constructor of this class.
 *
 * As key access for sensitive value decryption is only used a few times during server initialization,
 * this implementation trades efficiency for security by only keeping the key in memory with an
 * in-scope reference for a brief period of time (assuming callers do not maintain an in-scope reference).
 *
 * @see CryptoKeyProvider
 */
public class BootstrapFileCryptoKeyProvider implements CryptoKeyProvider {

    private static final Logger logger = LoggerFactory.getLogger(BootstrapFileCryptoKeyProvider.class);

    private final String bootstrapFile;

    /**
     * Construct a new instance backed by the contents of a bootstrap.conf file.
     *
     * @param bootstrapFilePath The path to the bootstrap.conf file for this instance of NiFi Registry.
     *                          Must not be null.
     */
    public BootstrapFileCryptoKeyProvider(final String bootstrapFilePath) {
        if (bootstrapFilePath == null) {
            throw new IllegalArgumentException(BootstrapFileCryptoKeyProvider.class.getSimpleName() + " cannot be initialized with null bootstrap file path.");
        }
        this.bootstrapFile = bootstrapFilePath;
    }

    /**
     * @return The bootstrap file path that backs this provider instance.
     */
    public String getBootstrapFile() {
        return bootstrapFile;
    }

    @Override
    public String getKey() throws MissingCryptoKeyException {
        try {
            return NiFiRegistryBootstrapUtils.extractKeyFromBootstrapFile(this.bootstrapFile);
        } catch (IOException ioe) {
            final String errMsg = "Loading the master crypto key from bootstrap file '" + bootstrapFile + "' failed due to IOException.";
            logger.warn(errMsg);
            throw new MissingCryptoKeyException(errMsg, ioe);
        }

    }

    @Override
    public String toString() {
        return "BootstrapFileCryptoKeyProvider{" +
                "bootstrapFile='" + bootstrapFile + '\'' +
                '}';
    }

}
