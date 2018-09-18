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
package org.apache.nifi.stream.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("Tests are time-based")
public class TestLeakyBucketThrottler {

    @Test(timeout = 10000)
    public void testOutputStreamInterface() throws IOException {
        // throttle rate at 1 MB/sec
        final LeakyBucketStreamThrottler throttler = new LeakyBucketStreamThrottler(1024 * 1024);

        final byte[] data = new byte[1024 * 1024 * 4];
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final OutputStream throttledOut = throttler.newThrottledOutputStream(baos)) {

            final long start = System.currentTimeMillis();
            throttledOut.write(data);
            throttler.close();
            final long millis = System.currentTimeMillis() - start;
            // should take 4 sec give or take
            assertTrue(millis > 3000);
            assertTrue(millis < 6000);
        }
    }

    @Test(timeout = 10000)
    public void testInputStreamInterface() throws IOException {

        final byte[] data = new byte[1024 * 1024 * 4];
     // throttle rate at 1 MB/sec
        try ( final LeakyBucketStreamThrottler throttler = new LeakyBucketStreamThrottler(1024 * 1024);
                final ByteArrayInputStream bais = new ByteArrayInputStream(data);
                final InputStream throttledIn = throttler.newThrottledInputStream(bais);
                final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

            final byte[] buffer = new byte[4096];
            final long start = System.currentTimeMillis();
            int len;
            while ((len = throttledIn.read(buffer)) > 0) {
                baos.write(buffer, 0, len);
            }

            final long millis = System.currentTimeMillis() - start;
            // should take 4 sec give or take
            assertTrue(millis > 3000);
            assertTrue(millis < 6000);
        }
    }

    @Test(timeout = 10000)
    public void testDirectInterface() throws IOException, InterruptedException {
        // throttle rate at 1 MB/sec
        try (final LeakyBucketStreamThrottler throttler = new LeakyBucketStreamThrottler(1024 * 1024);
                final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            // create 3 threads, each sending ~2 MB
            final List<Thread> threads = new ArrayList<Thread>();
            for (int i = 0; i < 3; i++) {
                final Thread t = new WriterThread(i, throttler, baos);
                threads.add(t);
            }

            final long start = System.currentTimeMillis();
            for (final Thread t : threads) {
                t.start();
            }

            for (final Thread t : threads) {
                t.join();
            }
            final long elapsed = System.currentTimeMillis() - start;

            throttler.close();

            // To send 15 MB, it should have taken at least 5 seconds and no more than 7 seconds, to
            // allow for busy-ness and the fact that we could write a tiny bit more than the limit.
            assertTrue(elapsed > 5000);
            assertTrue(elapsed < 7000);

            // ensure bytes were copied out appropriately
            assertEquals(3 * (2 * 1024 * 1024 + 1), baos.size());
            assertEquals((byte) 'A', baos.toByteArray()[baos.size() - 1]);
        }
    }

    private static class WriterThread extends Thread {

        private final int idx;
        private final byte[] data = new byte[1024 * 1024 * 2 + 1];
        private final LeakyBucketStreamThrottler throttler;
        private final OutputStream out;

        public WriterThread(final int idx, final LeakyBucketStreamThrottler throttler, final OutputStream out) {
            this.idx = idx;
            this.throttler = throttler;
            this.out = out;
            this.data[this.data.length - 1] = (byte) 'A';
        }

        @Override
        public void run() {
            long startMillis = System.currentTimeMillis();
            long bytesWritten = 0L;
            try {
                throttler.copy(new ByteArrayInputStream(data), out);
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
            long now = System.currentTimeMillis();
            long millisElapsed = now - startMillis;
            bytesWritten += data.length;
            float bytesPerSec = (float) bytesWritten / (float) millisElapsed * 1000F;
            System.out.println(idx + " : copied data at a rate of " + bytesPerSec + " bytes/sec");
        }
    }

}
