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
package org.apache.nifi.remote.client;

import org.apache.nifi.remote.util.PeerStatusCache;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class FilePeerPersistence extends AbstractPeerPersistence {

    private final File persistenceFile;

    public FilePeerPersistence(File persistenceFile) {
        this.persistenceFile = persistenceFile;
    }

    @Override
    public void save(final PeerStatusCache peerStatusCache) throws IOException {
        try (final OutputStream fos = new FileOutputStream(persistenceFile);
             final OutputStream out = new BufferedOutputStream(fos)) {
            write(peerStatusCache, line -> out.write(line.getBytes(StandardCharsets.UTF_8)));
        }
    }

    @Override
    public PeerStatusCache restore() throws IOException {
        try (final InputStream fis = new FileInputStream(persistenceFile);
             final BufferedReader reader = new BufferedReader(new InputStreamReader(fis))) {
            return restorePeerStatuses(reader, persistenceFile.lastModified());
        }
    }
}
