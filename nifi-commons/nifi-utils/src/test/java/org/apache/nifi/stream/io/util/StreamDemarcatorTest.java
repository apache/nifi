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
package org.apache.nifi.stream.io.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.junit.Test;

public class StreamDemarcatorTest {

    @Test
    public void validateInitializationFailure() {
        try {
            new StreamDemarcator(null, null, -1);
            fail();
        } catch (IllegalArgumentException e) {
            // success
        }

        try {
            new StreamDemarcator(mock(InputStream.class), null, -1);
            fail();
        } catch (IllegalArgumentException e) {
            // success
        }

        try {
            new StreamDemarcator(mock(InputStream.class), null, 10, -1);
            fail();
        } catch (IllegalArgumentException e) {
            // success
        }

        try {
            new StreamDemarcator(mock(InputStream.class), new byte[0], 10, 1);
            fail();
        } catch (IllegalArgumentException e) {
            // success
        }
    }

    @Test
    public void validateNoDelimiter() {
        String data = "Learn from yesterday, live for today, hope for tomorrow. The important thing is not to stop questioning.";
        ByteArrayInputStream is = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
        StreamDemarcator scanner = new StreamDemarcator(is, null, 1000);
        assertTrue(Arrays.equals(data.getBytes(StandardCharsets.UTF_8), scanner.nextToken()));
        // validate that subsequent invocations of nextToken() do not result in exception
        assertNull(scanner.nextToken());
        assertNull(scanner.nextToken());
    }

    @Test
    public void validateNoDelimiterSmallInitialBuffer() {
        String data = "Learn from yesterday, live for today, hope for tomorrow. The important thing is not to stop questioning.";
        ByteArrayInputStream is = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
        StreamDemarcator scanner = new StreamDemarcator(is, null, 1000, 1);
        assertTrue(Arrays.equals(data.getBytes(StandardCharsets.UTF_8), scanner.nextToken()));
    }

    @Test
    public void validateSingleByteDelimiter() {
        String data = "Learn from yesterday, live for today, hope for tomorrow. The important thing is not to stop questioning.";
        ByteArrayInputStream is = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
        StreamDemarcator scanner = new StreamDemarcator(is, ",".getBytes(StandardCharsets.UTF_8), 1000);
        assertTrue(Arrays.equals("Learn from yesterday".getBytes(StandardCharsets.UTF_8), scanner.nextToken()));
        assertTrue(Arrays.equals(" live for today".getBytes(StandardCharsets.UTF_8), scanner.nextToken()));
        assertTrue(Arrays.equals(" hope for tomorrow. The important thing is not to stop questioning.".getBytes(StandardCharsets.UTF_8), scanner.nextToken()));
        assertNull(scanner.nextToken());
    }

    @Test
    public void validateDelimiterAtTheBeginning() {
        String data = ",Learn from yesterday, live for today, hope for tomorrow. The important thing is not to stop questioning.";
        ByteArrayInputStream is = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
        StreamDemarcator scanner = new StreamDemarcator(is, ",".getBytes(StandardCharsets.UTF_8), 1000);
        assertTrue(Arrays.equals("Learn from yesterday".getBytes(StandardCharsets.UTF_8), scanner.nextToken()));
        assertTrue(Arrays.equals(" live for today".getBytes(StandardCharsets.UTF_8), scanner.nextToken()));
        assertTrue(Arrays.equals(" hope for tomorrow. The important thing is not to stop questioning.".getBytes(StandardCharsets.UTF_8), scanner.nextToken()));
        assertNull(scanner.nextToken());
    }

    @Test
    public void validateEmptyDelimiterSegments() {
        String data = ",,,,,Learn from yesterday, live for today, hope for tomorrow. The important thing is not to stop questioning.";
        ByteArrayInputStream is = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
        StreamDemarcator scanner = new StreamDemarcator(is, ",".getBytes(StandardCharsets.UTF_8), 1000);
        assertTrue(Arrays.equals("Learn from yesterday".getBytes(StandardCharsets.UTF_8), scanner.nextToken()));
        assertTrue(Arrays.equals(" live for today".getBytes(StandardCharsets.UTF_8), scanner.nextToken()));
        assertTrue(Arrays.equals(" hope for tomorrow. The important thing is not to stop questioning.".getBytes(StandardCharsets.UTF_8), scanner.nextToken()));
        assertNull(scanner.nextToken());
    }

    @Test
    public void validateSingleByteDelimiterSmallInitialBuffer() {
        String data = "Learn from yesterday, live for today, hope for tomorrow. The important thing is not to stop questioning.";
        ByteArrayInputStream is = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
        StreamDemarcator scanner = new StreamDemarcator(is, ",".getBytes(StandardCharsets.UTF_8), 1000, 2);
        assertTrue(Arrays.equals("Learn from yesterday".getBytes(StandardCharsets.UTF_8), scanner.nextToken()));
        assertTrue(Arrays.equals(" live for today".getBytes(StandardCharsets.UTF_8), scanner.nextToken()));
        assertTrue(Arrays.equals(" hope for tomorrow. The important thing is not to stop questioning.".getBytes(StandardCharsets.UTF_8), scanner.nextToken()));
        assertNull(scanner.nextToken());
    }

    @Test
    public void validateWithMultiByteDelimiter() {
        String data = "foodaabardaabazzz";
        ByteArrayInputStream is = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
        StreamDemarcator scanner = new StreamDemarcator(is, "daa".getBytes(StandardCharsets.UTF_8), 1000);
        assertTrue(Arrays.equals("foo".getBytes(StandardCharsets.UTF_8), scanner.nextToken()));
        assertTrue(Arrays.equals("bar".getBytes(StandardCharsets.UTF_8), scanner.nextToken()));
        assertTrue(Arrays.equals("bazzz".getBytes(StandardCharsets.UTF_8), scanner.nextToken()));
        assertNull(scanner.nextToken());
    }

    @Test
    public void validateWithMultiByteDelimiterAtTheBeginning() {
        String data = "daafoodaabardaabazzz";
        ByteArrayInputStream is = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
        StreamDemarcator scanner = new StreamDemarcator(is, "daa".getBytes(StandardCharsets.UTF_8), 1000);
        assertTrue(Arrays.equals("foo".getBytes(StandardCharsets.UTF_8), scanner.nextToken()));
        assertTrue(Arrays.equals("bar".getBytes(StandardCharsets.UTF_8), scanner.nextToken()));
        assertTrue(Arrays.equals("bazzz".getBytes(StandardCharsets.UTF_8), scanner.nextToken()));
        assertNull(scanner.nextToken());
    }

    @Test
    public void validateWithMultiByteDelimiterSmallInitialBuffer() {
        String data = "foodaabarffdaabazz";
        ByteArrayInputStream is = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
        StreamDemarcator scanner = new StreamDemarcator(is, "daa".getBytes(StandardCharsets.UTF_8), 1000, 1);
        assertTrue(Arrays.equals("foo".getBytes(StandardCharsets.UTF_8), scanner.nextToken()));
        assertTrue(Arrays.equals("barff".getBytes(StandardCharsets.UTF_8), scanner.nextToken()));
        assertTrue(Arrays.equals("bazz".getBytes(StandardCharsets.UTF_8), scanner.nextToken()));
        assertNull(scanner.nextToken());
    }

    @Test
    public void validateWithMultiByteCharsNoDelimiter() {
        String data = "僠THIS IS MY NEW TEXT.僠IT HAS A NEWLINE.";
        ByteArrayInputStream is = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
        StreamDemarcator scanner = new StreamDemarcator(is, null, 1000);
        byte[] next = scanner.nextToken();
        assertNotNull(next);
        assertEquals(data, new String(next, StandardCharsets.UTF_8));
        assertNull(scanner.nextToken());
    }

    @Test
    public void validateWithMultiByteCharsNoDelimiterSmallInitialBuffer() {
        String data = "僠THIS IS MY NEW TEXT.僠IT HAS A NEWLINE.";
        ByteArrayInputStream is = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
        StreamDemarcator scanner = new StreamDemarcator(is, null, 1000, 2);
        byte[] next = scanner.nextToken();
        assertNotNull(next);
        assertEquals(data, new String(next, StandardCharsets.UTF_8));
        assertNull(scanner.nextToken());
    }

    @Test
    public void validateWithComplexDelimiter() {
        String data = "THIS IS MY TEXT<MYDEIMITER>THIS IS MY NEW TEXT<MYDEIMITER>THIS IS MY NEWEST TEXT";
        ByteArrayInputStream is = new ByteArrayInputStream(data.getBytes());
        StreamDemarcator scanner = new StreamDemarcator(is, "<MYDEIMITER>".getBytes(StandardCharsets.UTF_8), 1000);
        assertEquals("THIS IS MY TEXT", new String(scanner.nextToken(), StandardCharsets.UTF_8));
        assertEquals("THIS IS MY NEW TEXT", new String(scanner.nextToken(), StandardCharsets.UTF_8));
        assertEquals("THIS IS MY NEWEST TEXT", new String(scanner.nextToken(), StandardCharsets.UTF_8));
        assertNull(scanner.nextToken());
    }

    @Test(expected = IllegalStateException.class)
    public void validateMaxBufferSize() {
        String data = "THIS IS MY TEXT<MY DEIMITER>THIS IS MY NEW TEXT<MY DEIMITER>THIS IS MY NEWEST TEXT";
        ByteArrayInputStream is = new ByteArrayInputStream(data.getBytes());
        StreamDemarcator scanner = new StreamDemarcator(is, "<MY DEIMITER>".getBytes(StandardCharsets.UTF_8), 20);
        scanner.nextToken();
    }

    @Test
    public void validateScannerHandlesNegativeOneByteInputsNoDelimiter() {
        ByteArrayInputStream is = new ByteArrayInputStream(new byte[] { 0, 0, 0, 0, -1, 0, 0, 0 });
        StreamDemarcator scanner = new StreamDemarcator(is, null, 20);
        byte[] b = scanner.nextToken();
        assertArrayEquals(b, new byte[] { 0, 0, 0, 0, -1, 0, 0, 0 });
    }

    @Test
    public void validateScannerHandlesNegativeOneByteInputs() {
        ByteArrayInputStream is = new ByteArrayInputStream(new byte[] { 0, 0, 0, 0, -1, 0, 0, 0 });
        StreamDemarcator scanner = new StreamDemarcator(is, "water".getBytes(StandardCharsets.UTF_8), 20, 1024);
        byte[] b = scanner.nextToken();
        assertArrayEquals(b, new byte[] { 0, 0, 0, 0, -1, 0, 0, 0 });
    }

    @Test
    public void verifyScannerHandlesNegativeOneByteDelimiter() {
        ByteArrayInputStream is = new ByteArrayInputStream(new byte[] { 0, 0, 0, 0, -1, 0, 0, 0 });
        StreamDemarcator scanner = new StreamDemarcator(is, new byte[] { -1 }, 20, 1024);
        assertArrayEquals(scanner.nextToken(), new byte[] { 0, 0, 0, 0 });
        assertArrayEquals(scanner.nextToken(), new byte[] { 0, 0, 0 });
    }
}
