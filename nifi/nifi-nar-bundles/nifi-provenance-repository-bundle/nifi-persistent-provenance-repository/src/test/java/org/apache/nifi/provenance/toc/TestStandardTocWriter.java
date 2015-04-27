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
package org.apache.nifi.provenance.toc;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.nifi.util.file.FileUtils;
import org.junit.Test;

public class TestStandardTocWriter {
    @Test
    public void testOverwriteEmptyFile() throws IOException {
        final File tocFile = new File("target/" + UUID.randomUUID().toString() + ".toc");
        try {
            assertTrue( tocFile.createNewFile() );

            try (final StandardTocWriter writer = new StandardTocWriter(tocFile, false, false)) {
            }
        } finally {
            FileUtils.deleteFile(tocFile, false);
        }
    }

}
