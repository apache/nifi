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
package org.apache.nifi.c2.client;

import org.apache.nifi.c2.client.api.IdGenerator;
import org.apache.nifi.c2.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class PersistentUuidGenerator implements IdGenerator {

    private static final Logger logger = LoggerFactory.getLogger(PersistentUuidGenerator.class);

    private final File persistenceLocation;

    public PersistentUuidGenerator(final File persistenceLocation) {
        this.persistenceLocation = persistenceLocation;
    }

    @Override
    public String generate() {
        if (this.persistenceLocation.exists()) {
            return readFile();
        } else {
            return makeFile();
        }
    }

    private String readFile() {
        try {
            final List<String> fileLines = Files.readAllLines(persistenceLocation.toPath());
            if (fileLines.size() != 1) {
                throw new IllegalStateException(String.format("The file %s for the persisted identifier has the incorrect format.", persistenceLocation));
            }
            final String uuid = fileLines.get(0);
            return uuid;
        } catch (IOException e) {
            throw new IllegalStateException(String.format("Could not read file %s for persisted identifier.", persistenceLocation), e);

        }
    }

    private String makeFile() {
        try {
            final File parentDirectory = persistenceLocation.getParentFile();
            FileUtils.ensureDirectoryExistAndCanAccess(parentDirectory);
            final String uuid = UUID.randomUUID().toString();
            Files.write(persistenceLocation.toPath(), Arrays.asList(uuid));
            logger.debug("Created identifier {} at {}", uuid, persistenceLocation);
            return uuid;
        } catch (IOException e) {
            throw new IllegalStateException(String.format("Could not create file %s as persistence file.", persistenceLocation), e);
        }
    }
}
