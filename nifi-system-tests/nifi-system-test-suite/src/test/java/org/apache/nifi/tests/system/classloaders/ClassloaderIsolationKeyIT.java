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

package org.apache.nifi.tests.system.classloaders;

import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;

public class ClassloaderIsolationKeyIT extends NiFiSystemIT {

    @Test
    public void testClassloaderChanges() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        final ProcessorEntity firstCounter = getClientUtil().createProcessor("WriteFlowFileCountToFile");
        final ProcessorEntity secondCounter = getClientUtil().createProcessor("WriteFlowFileCountToFile");

        getClientUtil().createConnection(generate, firstCounter, "success");
        getClientUtil().createConnection(firstCounter, secondCounter, "success");
        getClientUtil().setAutoTerminatedRelationships(secondCounter, "success");

        getClientUtil().updateProcessorProperties(firstCounter, Collections.singletonMap("File to Write", "count1.txt"));
        getClientUtil().updateProcessorProperties(secondCounter, Collections.singletonMap("File to Write", "count2.txt"));

        getClientUtil().updateProcessorProperties(firstCounter, Collections.singletonMap("Isolation Key", "key-1"));
        getClientUtil().updateProcessorProperties(secondCounter, Collections.singletonMap("Isolation Key", "key-1"));

        getClientUtil().startProcessGroupComponents("root");

        final File nifiHome = getNiFiInstance().getInstanceDirectory();
        final File firstCountFile = new File(nifiHome, "count1.txt");
        final File secondCountFile = new File(nifiHome, "count2.txt");

        waitForCount(firstCountFile, 1);
        waitForCount(secondCountFile, 2);

        // Stop processors and change Isolation Key for the first processor. This should result in a new classloader which will result in
        // the processor having a new statically defined AtomicLong. This means we'll now get a new count of 1, while count2.txt should increment again to a value of 3.
        getClientUtil().stopProcessGroupComponents("root");
        getClientUtil().updateProcessorProperties(firstCounter, Collections.singletonMap("Isolation Key", "key-2"));
        getClientUtil().startProcessGroupComponents("root");

        waitForCount(firstCountFile, 1);
        waitForCount(secondCountFile, 3);

        // Change key back to key-1, which should result in going back to the original statically defined AtomicLong.
        getClientUtil().stopProcessGroupComponents("root");
        getClientUtil().updateProcessorProperties(firstCounter, Collections.singletonMap("Isolation Key", "key-1"));
        getClientUtil().startProcessGroupComponents("root");

        waitForCount(firstCountFile, 4);
        waitForCount(secondCountFile, 5);

        // Change both processors to a new Isolation Key and restart. This should result in both processors still having the same ClassLoader (and therefore the same
        // AtomicLong after restart).
        getClientUtil().stopProcessGroupComponents("root");
        getClientUtil().updateProcessorProperties(firstCounter, Collections.singletonMap("Isolation Key", "other"));
        getClientUtil().updateProcessorProperties(secondCounter, Collections.singletonMap("Isolation Key", "other"));
        getNiFiInstance().stop();
        getNiFiInstance().start(true);

        getClientUtil().startProcessGroupComponents("root");

        waitForCount(firstCountFile, 1);
        waitForCount(secondCountFile, 2);
    }

    private void waitForCount(final File file, final long expectedValue) throws InterruptedException {
        waitFor(() -> {
            try {
                return getCount(file) == expectedValue;
            } catch (IOException e) {
                return false;
            }
        });
    }

    private long getCount(final File file) throws IOException {
        final byte[] fileContents = Files.readAllBytes(file.toPath());
        return Long.parseLong(new String(fileContents, StandardCharsets.UTF_8));
    }
}
