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
package org.apache.nifi.controller.repository;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.repository.claim.StandardContentClaim;
import org.apache.nifi.controller.repository.claim.StandardResourceClaimManager;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

public class TestVolatileContentRepository {

    private ResourceClaimManager claimManager;

    @Before
    public void setup() {
        claimManager = new StandardResourceClaimManager();
    }

    @Test
    public void testRedirects() throws IOException {
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, TestVolatileContentRepository.class.getResource("/conf/nifi.properties").getFile());
        final Map<String, String> addProps = new HashMap<>();
        addProps.put(VolatileContentRepository.MAX_SIZE_PROPERTY, "10 MB");
        final NiFiProperties nifiProps = NiFiProperties.createBasicNiFiProperties(null, addProps);
        final VolatileContentRepository contentRepo = new VolatileContentRepository(nifiProps);
        contentRepo.initialize(claimManager);
        final ContentClaim claim = contentRepo.create(true);
        final OutputStream out = contentRepo.write(claim);

        final byte[] oneK = new byte[1024];
        Arrays.fill(oneK, (byte) 55);

        // Write 10 MB to the repo
        for (int i = 0; i < 10240; i++) {
            out.write(oneK);
        }

        try {
            out.write(1);
            Assert.fail("Expected to be out of space on content repo");
        } catch (final IOException e) {
        }

        try {
            out.write(1);
            Assert.fail("Expected to be out of space on content repo");
        } catch (final IOException e) {
        }

        final ContentRepository mockRepo = Mockito.mock(ContentRepository.class);
        contentRepo.setBackupRepository(mockRepo);
        final ResourceClaim resourceClaim = claimManager.newResourceClaim("container", "section", "1000", true, false);
        final ContentClaim contentClaim = new StandardContentClaim(resourceClaim, 0L);
        Mockito.when(mockRepo.create(Matchers.anyBoolean())).thenReturn(contentClaim);

        final ByteArrayOutputStream overflowStream = new ByteArrayOutputStream();
        Mockito.when(mockRepo.write(Matchers.any(ContentClaim.class))).thenReturn(overflowStream);
        out.write(10);

        assertEquals(1024 * 1024 * 10 + 1, overflowStream.size());
        final byte[] overflowBuffer = overflowStream.toByteArray();

        assertEquals(55, overflowBuffer[0]);
        for (int i = 0; i < overflowBuffer.length; i++) {
            if (i == overflowBuffer.length - 1) {
                assertEquals(10, overflowBuffer[i]);
            } else {
                assertEquals(55, overflowBuffer[i]);
            }
        }
    }

    @Test
    public void testMemoryIsFreed() throws IOException, InterruptedException {
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, TestVolatileContentRepository.class.getResource("/conf/nifi.properties").getFile());
        final Map<String, String> addProps = new HashMap<>();
        addProps.put(VolatileContentRepository.MAX_SIZE_PROPERTY, "11 MB");
        final NiFiProperties nifiProps = NiFiProperties.createBasicNiFiProperties(null, addProps);
        final VolatileContentRepository contentRepo = new VolatileContentRepository(nifiProps);

        contentRepo.initialize(claimManager);

        final byte[] oneK = new byte[1024];
        Arrays.fill(oneK, (byte) 55);

        final ExecutorService exec = Executors.newFixedThreadPool(10);
        for (int t = 0; t < 10; t++) {
            final Runnable r = new Runnable() {
                @Override
                public void run() {
                    try {
                        for (int j = 0; j < 10000; j++) {
                            final ContentClaim claim = contentRepo.create(true);
                            final OutputStream out = contentRepo.write(claim);

                            // Write 1 MB to the repo
                            for (int i = 0; i < 1024; i++) {
                                out.write(oneK);
                            }

                            final int count = contentRepo.decrementClaimantCount(claim);
                            if (count <= 0) {
                                contentRepo.remove(claim);
                            }
                        }
                    } catch (final Exception e) {
                        e.printStackTrace();
                    }
                }
            };

            exec.submit(r);
        }

        exec.shutdown();
        exec.awaitTermination(100000, TimeUnit.MINUTES);
    }

    @Test
    public void testSimpleReadWrite() throws IOException {
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, TestVolatileContentRepository.class.getResource("/conf/nifi.properties").getFile());
        final Map<String, String> addProps = new HashMap<>();
        addProps.put(VolatileContentRepository.MAX_SIZE_PROPERTY, "11 MB");
        final NiFiProperties nifiProps = NiFiProperties.createBasicNiFiProperties(null, addProps);
        final VolatileContentRepository contentRepo = new VolatileContentRepository(nifiProps);
        contentRepo.initialize(claimManager);
        final ContentClaim claim = contentRepo.create(true);

        final OutputStream out = contentRepo.write(claim);
        final int byteCount = 2398473 * 4;
        final byte[] x = new byte[4];
        x[0] = 48;
        x[1] = 29;
        x[2] = 49;
        x[3] = 51;

        for (int i = 0; i < byteCount / 4; i++) {
            out.write(x);
        }

        out.close();

        final InputStream in = contentRepo.read(claim);
        for (int i = 0; i < byteCount; i++) {
            final int val = in.read();
            final int index = i % 4;
            final byte expectedVal = x[index];
            assertEquals(expectedVal, val);
        }

        assertEquals(-1, in.read());
    }
}
