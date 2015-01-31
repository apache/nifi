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
package org.apache.nifi.provenance.rollover;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.nifi.stream.io.GZIPOutputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.provenance.lucene.IndexingAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompressionAction implements RolloverAction {

    private static final Logger logger = LoggerFactory.getLogger(IndexingAction.class);

    @Override
    public File execute(final File fileRolledOver) throws IOException {
        final File gzFile = new File(fileRolledOver.getParent(), fileRolledOver.getName() + ".gz");
        try (final FileInputStream in = new FileInputStream(fileRolledOver);
                final OutputStream fos = new FileOutputStream(gzFile);
                final GZIPOutputStream gzipOut = new GZIPOutputStream(fos, 1)) {
            StreamUtils.copy(in, gzipOut);
            in.getFD().sync();
        }

        boolean deleted = false;
        for (int i = 0; i < 10 && !deleted; i++) {
            deleted = fileRolledOver.delete();
        }

        logger.info("Finished compressing Provenance Log File {}", fileRolledOver);
        return gzFile;
    }

    @Override
    public boolean hasBeenPerformed(final File fileRolledOver) {
        return fileRolledOver.getName().contains(".gz");
    }

}
