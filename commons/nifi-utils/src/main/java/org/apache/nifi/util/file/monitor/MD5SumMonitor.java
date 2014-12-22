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
package org.apache.nifi.util.file.monitor;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5SumMonitor implements UpdateMonitor {

    @Override
    public Object getCurrentState(final Path path) throws IOException {
        final MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("MD5");
        } catch (final NoSuchAlgorithmException nsae) {
            throw new AssertionError(nsae);
        }

        try (final FileInputStream fis = new FileInputStream(path.toFile())) {
            int len;
            final byte[] buffer = new byte[8192];
            while ((len = fis.read(buffer)) > -1) {
                if (len > 0) {
                    digest.update(buffer, 0, len);
                }
            }
        }

        // Return a ByteBuffer instead of byte[] because we want equals() to do a deep equality
        return ByteBuffer.wrap(digest.digest());
    }

}
