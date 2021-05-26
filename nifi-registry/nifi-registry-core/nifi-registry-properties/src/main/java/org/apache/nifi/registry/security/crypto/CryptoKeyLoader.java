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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.stream.Stream;

public class CryptoKeyLoader {

    private static final Logger logger = LoggerFactory.getLogger(CryptoKeyLoader.class);

    private static final String BOOTSTRAP_KEY_PREFIX = "nifi.registry.bootstrap.sensitive.key=";

    /**
     * Returns the key (if any) used to encrypt sensitive properties.
     * The key extracted from the bootstrap.conf file at the specified location.
     *
     * @param bootstrapPath the path to the bootstrap file
     * @return the key in hexadecimal format, or {@link CryptoKeyProvider#EMPTY_KEY} if the key is null or empty
     * @throws IOException if the file is not readable
     */
    public static String extractKeyFromBootstrapFile(String bootstrapPath) throws IOException {
        File bootstrapFile;
        if (StringUtils.isBlank(bootstrapPath)) {
            logger.error("Cannot read from bootstrap.conf file to extract encryption key; location not specified");
            throw new IOException("Cannot read from bootstrap.conf without file location");
        } else {
            bootstrapFile = new File(bootstrapPath);
        }

        String keyValue;
        if (bootstrapFile.exists() && bootstrapFile.canRead()) {
            try (Stream<String> stream = Files.lines(Paths.get(bootstrapFile.getAbsolutePath()))) {
                Optional<String> keyLine = stream.filter(l -> l.startsWith(BOOTSTRAP_KEY_PREFIX)).findFirst();
                if (keyLine.isPresent()) {
                    keyValue = keyLine.get().split("=", 2)[1];
                    keyValue = checkHexKey(keyValue);
                } else {
                    keyValue = CryptoKeyProvider.EMPTY_KEY;
                }
            } catch (IOException e) {
                logger.error("Cannot read from bootstrap.conf file at {} to extract encryption key", bootstrapFile.getAbsolutePath());
                throw new IOException("Cannot read from bootstrap.conf", e);
            }
        } else {
            logger.error("Cannot read from bootstrap.conf file at {} to extract encryption key -- file is missing or permissions are incorrect", bootstrapFile.getAbsolutePath());
            throw new IOException("Cannot read from bootstrap.conf");
        }

        if (CryptoKeyProvider.EMPTY_KEY.equals(keyValue)) {
            logger.info("No encryption key present in the bootstrap.conf file at {}", bootstrapFile.getAbsolutePath());
        }

        return keyValue;
    }

    private static String checkHexKey(String input) {
        if (input == null || input.trim().isEmpty()) {
            logger.debug("Checking the hex key value that was loaded determined the key is empty.");
            return CryptoKeyProvider.EMPTY_KEY;
        }
        return input;
    }

}
