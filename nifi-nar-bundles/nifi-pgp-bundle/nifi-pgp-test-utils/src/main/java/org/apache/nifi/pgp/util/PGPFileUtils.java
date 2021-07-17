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
package org.apache.nifi.pgp.util;

import org.bouncycastle.bcpg.ArmoredOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Pretty Good Privacy File Utilities
 */
public class PGPFileUtils {
    private static final String FILE_EXTENSION = ".key";

    /**
     * Get Key File created in temporary directory to be deleted after completion
     *
     * @param bytes Bytes to be written
     * @return File created in temporary directory
     * @throws IOException Thrown on File.createTempFile()
     */
    public static File getKeyFile(final byte[] bytes) throws IOException {
        final File encodedFile = File.createTempFile(PGPFileUtils.class.getSimpleName(), FILE_EXTENSION);
        encodedFile.deleteOnExit();

        try (final OutputStream outputStream = new FileOutputStream(encodedFile)) {
            outputStream.write(bytes);
            return encodedFile;
        }
    }

    /**
     * Get ASCII Armored string from bytes
     *
     * @param bytes Bytes to written
     * @return ASCII Armored string
     * @throws IOException Thrown on ArmoredOutputStream.write()
     */
    public static String getArmored(final byte[] bytes) throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (final ArmoredOutputStream armoredOutputStream = new ArmoredOutputStream(outputStream)) {
            armoredOutputStream.write(bytes);
        }
        return outputStream.toString();
    }
}
