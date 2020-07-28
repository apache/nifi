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

import org.apache.nifi.stream.io.RepeatingInputStream;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class TestLineDemarcator {

    @Test
    public void testSingleCharacterLines() throws IOException {
        final String input = "A\nB\nC\rD\r\nE\r\nF\r\rG";

        final List<String> lines = getLines(input);
        assertEquals(Arrays.asList("A\n", "B\n", "C\r", "D\r\n", "E\r\n", "F\r", "\r", "G"), lines);
    }


    @Test
    public void testEmptyStream() throws IOException {
        final List<String> lines = getLines("");
        assertEquals(Collections.emptyList(), lines);
    }

    @Test
    public void testOnlyEmptyLines() throws IOException {
        final String input = "\r\r\r\n\n\n\r\n";

        final List<String> lines = getLines(input);
        assertEquals(Arrays.asList("\r", "\r", "\r\n", "\n", "\n", "\r\n"), lines);
    }

    @Test
    public void testOnBufferSplit() throws IOException {
        final String input = "ABC\r\nXYZ";
        final List<String> lines = getLines(input, 10, 4);

        assertEquals(Arrays.asList("ABC\r\n", "XYZ"), lines);
    }

    @Test
    public void testEndsWithCarriageReturn() throws IOException {
        final List<String> lines = getLines("ABC\r");
        assertEquals(Arrays.asList("ABC\r"), lines);
    }

    @Test
    public void testEndsWithNewLine() throws IOException {
        final List<String> lines = getLines("ABC\n");
        assertEquals(Arrays.asList("ABC\n"), lines);
    }

    @Test
    public void testEndsWithCarriageReturnNewLine() throws IOException {
        final List<String> lines = getLines("ABC\r\n");
        assertEquals(Arrays.asList("ABC\r\n"), lines);
    }

    @Test
    public void testReadAheadInIsEol() throws IOException {
        final String input = "he\ra-to-a\rb-to-b\rc-to-c\r\nd-to-d";
        final List<String> lines = getLines(input, 10, 10);

        assertEquals(Arrays.asList("he\r", "a-to-a\r", "b-to-b\r", "c-to-c\r\n", "d-to-d"), lines);
    }

    @Test
    public void testFirstCharMatchOnly() throws IOException {
        final List<String> lines = getLines("\nThe quick brown fox jumped over the lazy dog.");
        assertEquals(Arrays.asList("\n", "The quick brown fox jumped over the lazy dog."), lines);
    }

    @Test
    @Ignore("Intended only for manual testing. While this can take a while to run, it can be very helpful for manual testing before and after a change to the class. However, we don't want this to " +
        "run in automated tests because we have no way to compare from one run to another, so it will only slow down automated tests.")
    public void testPerformance() throws IOException {
        final String lines = "The\nquick\nbrown\nfox\njumped\nover\nthe\nlazy\ndog.\r\n\n";
        final byte[] bytes = lines.getBytes(StandardCharsets.UTF_8);

        for (int i=0; i < 100; i++) {
            final long start = System.nanoTime();

            long count = 0;
            try (final InputStream in = new RepeatingInputStream(bytes, 1_000_000);
                 final LineDemarcator demarcator = new LineDemarcator(in, StandardCharsets.UTF_8, 8192, 8192)) {

                while (demarcator.nextLine() != null) {
                    count++;
                }
            }

            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            System.out.println("Took " + millis + " millis to demarcate " + count + " lines");
        }
    }

    private List<String> getLines(final String text) throws IOException {
        return getLines(text, 8192, 8192);
    }

    private List<String> getLines(final String text, final int maxDataSize, final int bufferSize) throws IOException {
        final byte[] bytes = text.getBytes(StandardCharsets.UTF_8);

        final List<String> lines = new ArrayList<>();

        try (final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
             final Reader reader = new InputStreamReader(bais, StandardCharsets.UTF_8);
             final LineDemarcator demarcator = new LineDemarcator(reader, maxDataSize, bufferSize)) {

            String line;
            while ((line = demarcator.nextLine()) != null) {
                lines.add(line);
            }
        }

        return lines;
    }

}
