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
package org.apache.nifi.controller.repository.io;

import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.metrics.NopPerformanceTracker;
import org.apache.nifi.stream.io.StreamUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class TestContentClaimInputStream {

    private ContentRepository repo;
    private ContentClaim contentClaim;
    private AtomicBoolean closed = new AtomicBoolean();

    @BeforeEach
    public void setup() throws IOException {
        repo = mock(ContentRepository.class);
        contentClaim = mock(ContentClaim.class);

        closed.set(false);
        Mockito.when(repo.read(contentClaim)).thenAnswer(invocation -> new ByteArrayInputStream("hello".getBytes()) {
            @Override
            public void close() throws IOException {
                super.close();
                closed.set(true);
            }
        });
    }


    @Test
    public void testStreamCreatedFromRepository() throws IOException {
        final ContentClaimInputStream in = new ContentClaimInputStream(repo, contentClaim, 0L, new NopPerformanceTracker());

        final byte[] buff = new byte[5];
        StreamUtils.fillBuffer(in, buff);

        Mockito.verify(repo).read(contentClaim);
        Mockito.verifyNoMoreInteractions(repo);

        final String contentRead = new String(buff);
        assertEquals("hello", contentRead);

        assertEquals(5, in.getBytesConsumed());
        assertFalse(closed.get());

        // Ensure that underlying stream is closed
        in.close();
        assertTrue(closed.get());
    }


    @Test
    public void testThatContentIsSkipped() throws IOException {
        final ContentClaimInputStream in = new ContentClaimInputStream(repo, contentClaim, 3L, new NopPerformanceTracker());

        final byte[] buff = new byte[2];
        StreamUtils.fillBuffer(in, buff);

        Mockito.verify(repo).read(contentClaim);
        Mockito.verifyNoMoreInteractions(repo);

        final String contentRead = new String(buff);
        assertEquals("lo", contentRead);

        assertEquals(2, in.getBytesConsumed());
        assertFalse(closed.get());

        // Ensure that underlying stream is closed
        in.close();
        assertTrue(closed.get());
    }


    @Test
    public void testRereadEntireClaim() throws IOException {
        final ContentClaimInputStream in = new ContentClaimInputStream(repo, contentClaim, 0L, new NopPerformanceTracker());

        final byte[] buff = new byte[5];

        final int invocations = 10;
        for (int i = 0; i < invocations; i++) {
            in.mark(5);

            StreamUtils.fillBuffer(in, buff, true);

            final String contentRead = new String(buff);
            assertEquals("hello", contentRead);

            assertEquals(5 * (i + 1), in.getBytesConsumed());
            assertEquals(5, in.getCurrentOffset());
            assertEquals(-1, in.read());

            in.reset();
        }

        Mockito.verify(repo).read(contentClaim);
        Mockito.verifyNoMoreInteractions(repo);

        // Ensure that underlying stream is closed
        in.close();
        assertTrue(closed.get());
    }


    @Test
    public void testMultipleResetCallsAfterMark() throws IOException {
        final ContentClaimInputStream in = new ContentClaimInputStream(repo, contentClaim, 0L, new NopPerformanceTracker());

        final byte[] buff = new byte[5];

        final int invocations = 10;
        in.mark(5);

        for (int i = 0; i < invocations; i++) {
            StreamUtils.fillBuffer(in, buff, true);

            final String contentRead = new String(buff);
            assertEquals("hello", contentRead);

            assertEquals(5 * (i + 1), in.getBytesConsumed());
            assertEquals(5, in.getCurrentOffset());
            assertEquals(-1, in.read());

            in.reset();
        }

        Mockito.verify(repo).read(contentClaim);
        Mockito.verifyNoMoreInteractions(repo);

        // Ensure that underlying stream is closed
        in.close();
        assertTrue(closed.get());
    }


    @Test
    public void testRereadWithOffset() throws IOException {
        final ContentClaimInputStream in = new ContentClaimInputStream(repo, contentClaim, 3L, new NopPerformanceTracker());

        final byte[] buff = new byte[2];

        final int invocations = 10;
        for (int i = 0; i < invocations; i++) {
            in.mark(5);

            StreamUtils.fillBuffer(in, buff, true);

            final String contentRead = new String(buff);
            assertEquals("lo", contentRead);

            assertEquals(2 * (i + 1), in.getBytesConsumed());
            assertEquals(5, in.getCurrentOffset());
            assertEquals(-1, in.read());

            in.reset();
        }

        Mockito.verify(repo).read(contentClaim);
        Mockito.verifyNoMoreInteractions(repo);

        // Ensure that underlying stream is closed
        in.close();
        assertTrue(closed.get());
    }


    @Test
    public void testRereadBiggerThanBuffer() throws IOException {
        final ContentClaimInputStream in = new ContentClaimInputStream(repo, contentClaim, 0L, new NopPerformanceTracker());

        final byte[] buff = new byte[5];

        final int invocations = 10;
        in.mark(2);

        for (int i = 0; i < invocations; i++) {
            StreamUtils.fillBuffer(in, buff, true);

            final String contentRead = new String(buff);
            assertEquals("hello", contentRead);

            assertEquals(5 * (i + 1), in.getBytesConsumed());
            assertEquals(5, in.getCurrentOffset());
            assertEquals(-1, in.read());

            in.reset();
        }

        Mockito.verify(repo, Mockito.times(invocations + 1)).read(contentClaim);
        Mockito.verifyNoMoreInteractions(repo);

        // Ensure that underlying stream is closed
        in.close();
        assertTrue(closed.get());
    }


    @Test
    public void testBigReadAfterSmallRereads() throws IOException {
        // this has to be bigger than the default buffer size of BufferedInputStream
        final int bigReadSize = 65_000;

        final byte[] source = new byte[bigReadSize];

        Mockito.when(repo.read(contentClaim)).thenAnswer(invocation -> {
            ByteArrayInputStream is = new ByteArrayInputStream(source) {
                @Override
                public void close() throws IOException {
                    super.close();
                    closed.set(true);
                }
            };
            // wrap it, because ByteArrayInputStream throws no exception when being read after a close
            return new BufferedInputStream(is);
        });

        final ContentClaimInputStream in = new ContentClaimInputStream(repo, contentClaim, 100L, repo.read(contentClaim), new NopPerformanceTracker());

        int invocations = 5;
        for (int i = 0; i < invocations; i++) {
            in.mark(1);
            in.read();

            assertEquals(i + 1, in.getBytesConsumed());
            assertEquals(101, in.getCurrentOffset());

            in.reset();
        }

        byte[] buff = new byte[bigReadSize];
        // Force the buffer to read from the delegate stream by reading all the data and therefore
        // going over the default buffer size.
        in.read(buff);
        in.reset();

        Mockito.verify(repo, Mockito.times(2)).read(contentClaim);
        Mockito.verifyNoMoreInteractions(repo);

        in.close();
        assertTrue(closed.get());
    }
}
