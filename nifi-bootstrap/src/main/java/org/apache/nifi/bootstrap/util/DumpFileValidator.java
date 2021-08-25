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
package org.apache.nifi.bootstrap.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class DumpFileValidator {

    private static final Logger logger = LoggerFactory.getLogger(DumpFileValidator.class);

    private DumpFileValidator() {
    }

    public static boolean validate(final String filePath) {
        try {
            final Path path = Paths.get(filePath);
            return checkFileCanBeCreated(path);
        } catch (InvalidPathException e) {
            System.err.println("Invalid filename. The command parameters are: status-history <number of days> <dumpFile>");
            return false;
        }
    }

    private static boolean checkFileCanBeCreated(final Path path) {
        try (final FileOutputStream outputStream = new FileOutputStream(path.toString());
             final Closeable onClose = () -> Files.delete(path)) {
        } catch (FileNotFoundException e) {
            System.err.println("Invalid filename or there's no write permission to the currently selected file path.");
            return false;
        } catch (IOException e) {
            logger.error("Could not delete file while validating file path.");
        }
        return true;
    }
}
