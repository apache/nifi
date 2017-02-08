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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.nifi.stream.io.util.TextLineDemarcator.OffsetInfo;
import org.junit.Test;

public class TextLineDemarcatorTest {

    @Test(expected = IllegalArgumentException.class)
    public void nullStream() {
        new TextLineDemarcator(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void illegalBufferSize() {
        new TextLineDemarcator(mock(InputStream.class), -234);
    }

    @Test
    public void emptyStreamNoStartWithFilter() {
        String data = "";
        InputStream is = stringToIs(data);
        TextLineDemarcator demarcator = new TextLineDemarcator(is);
        assertNull(demarcator.nextOffsetInfo());
    }


    @Test
    public void emptyStreamAndStartWithFilter() {
        String data = "";
        InputStream is = stringToIs(data);
        TextLineDemarcator demarcator = new TextLineDemarcator(is);
        assertNull(demarcator.nextOffsetInfo("hello".getBytes()));
    }

    // this test has no assertions. It's success criteria is validated by lack
    // of failure (see NIFI-3278)
    @Test
    public void endsWithCRWithBufferLengthEqualStringLengthA() {
        String str = "\r";
        InputStream is = stringToIs(str);
        TextLineDemarcator demarcator = new TextLineDemarcator(is, str.length());
        while (demarcator.nextOffsetInfo() != null) {
        }
    }

    @Test
    public void endsWithCRWithBufferLengthEqualStringLengthB() {
        String str = "abc\r";
        InputStream is = stringToIs(str);
        TextLineDemarcator demarcator = new TextLineDemarcator(is, str.length());
        while (demarcator.nextOffsetInfo() != null) {
        }
    }

    @Test
    public void singleCR() {
        InputStream is = stringToIs("\r");
        TextLineDemarcator demarcator = new TextLineDemarcator(is);
        OffsetInfo offsetInfo = demarcator.nextOffsetInfo();
        assertEquals(0, offsetInfo.getStartOffset());
        assertEquals(1, offsetInfo.getLength());
        assertEquals(1, offsetInfo.getCrlfLength());
        assertTrue(offsetInfo.isStartsWithMatch());
    }

    @Test
    public void singleLF() {
        InputStream is = stringToIs("\n");
        TextLineDemarcator demarcator = new TextLineDemarcator(is);
        OffsetInfo offsetInfo = demarcator.nextOffsetInfo();
        assertEquals(0, offsetInfo.getStartOffset());
        assertEquals(1, offsetInfo.getLength());
        assertEquals(1, offsetInfo.getCrlfLength());
        assertTrue(offsetInfo.isStartsWithMatch());
    }

    @Test
    // essentially validates the internal 'isEol()' operation to ensure it will perform read-ahead
    public void crlfWhereLFdoesNotFitInInitialBuffer() throws Exception {
        InputStream is = stringToIs("oleg\r\njoe");
        TextLineDemarcator demarcator = new TextLineDemarcator(is, 5);
        OffsetInfo offsetInfo = demarcator.nextOffsetInfo();
        assertEquals(0, offsetInfo.getStartOffset());
        assertEquals(6, offsetInfo.getLength());
        assertEquals(2, offsetInfo.getCrlfLength());
        assertTrue(offsetInfo.isStartsWithMatch());

        offsetInfo = demarcator.nextOffsetInfo();
        assertEquals(6, offsetInfo.getStartOffset());
        assertEquals(3, offsetInfo.getLength());
        assertEquals(0, offsetInfo.getCrlfLength());
        assertTrue(offsetInfo.isStartsWithMatch());
    }

    @Test
    public void mixedCRLF() throws Exception {
        InputStream is = stringToIs("oleg\rjoe\njack\r\nstacymike\r\n");
        TextLineDemarcator demarcator = new TextLineDemarcator(is, 4);
        OffsetInfo offsetInfo = demarcator.nextOffsetInfo();
        assertEquals(0, offsetInfo.getStartOffset());
        assertEquals(5, offsetInfo.getLength());
        assertEquals(1, offsetInfo.getCrlfLength());
        assertTrue(offsetInfo.isStartsWithMatch());

        offsetInfo = demarcator.nextOffsetInfo();
        assertEquals(5, offsetInfo.getStartOffset());
        assertEquals(4, offsetInfo.getLength());
        assertEquals(1, offsetInfo.getCrlfLength());
        assertTrue(offsetInfo.isStartsWithMatch());

        offsetInfo = demarcator.nextOffsetInfo();
        assertEquals(9, offsetInfo.getStartOffset());
        assertEquals(6, offsetInfo.getLength());
        assertEquals(2, offsetInfo.getCrlfLength());
        assertTrue(offsetInfo.isStartsWithMatch());

        offsetInfo = demarcator.nextOffsetInfo();
        assertEquals(15, offsetInfo.getStartOffset());
        assertEquals(11, offsetInfo.getLength());
        assertEquals(2, offsetInfo.getCrlfLength());
        assertTrue(offsetInfo.isStartsWithMatch());
    }

    @Test
    public void consecutiveAndMixedCRLF() throws Exception {
        InputStream is = stringToIs("oleg\r\r\njoe\n\n\rjack\n\r\nstacymike\r\n\n\n\r");
        TextLineDemarcator demarcator = new TextLineDemarcator(is, 4);

        OffsetInfo offsetInfo = demarcator.nextOffsetInfo(); // oleg\r
        assertEquals(5, offsetInfo.getLength());
        assertEquals(1, offsetInfo.getCrlfLength());

        offsetInfo = demarcator.nextOffsetInfo();        // \r\n
        assertEquals(2, offsetInfo.getLength());
        assertEquals(2, offsetInfo.getCrlfLength());

        offsetInfo = demarcator.nextOffsetInfo();        // joe\n
        assertEquals(4, offsetInfo.getLength());
        assertEquals(1, offsetInfo.getCrlfLength());

        offsetInfo = demarcator.nextOffsetInfo();        // \n
        assertEquals(1, offsetInfo.getLength());
        assertEquals(1, offsetInfo.getCrlfLength());

        offsetInfo = demarcator.nextOffsetInfo();        // \r
        assertEquals(1, offsetInfo.getLength());
        assertEquals(1, offsetInfo.getCrlfLength());

        offsetInfo = demarcator.nextOffsetInfo();        // jack\n
        assertEquals(5, offsetInfo.getLength());
        assertEquals(1, offsetInfo.getCrlfLength());

        offsetInfo = demarcator.nextOffsetInfo();        // \r\n
        assertEquals(2, offsetInfo.getLength());
        assertEquals(2, offsetInfo.getCrlfLength());

        offsetInfo = demarcator.nextOffsetInfo();        // stacymike\r\n
        assertEquals(11, offsetInfo.getLength());
        assertEquals(2, offsetInfo.getCrlfLength());

        offsetInfo = demarcator.nextOffsetInfo();        // \n
        assertEquals(1, offsetInfo.getLength());
        assertEquals(1, offsetInfo.getCrlfLength());

        offsetInfo = demarcator.nextOffsetInfo();        // \n
        assertEquals(1, offsetInfo.getLength());
        assertEquals(1, offsetInfo.getCrlfLength());

        offsetInfo = demarcator.nextOffsetInfo();        // \r
        assertEquals(1, offsetInfo.getLength());
        assertEquals(1, offsetInfo.getCrlfLength());
    }

    @Test
    public void startWithNoMatchOnWholeStream() throws Exception {
        InputStream is = stringToIs("oleg\rjoe\njack\r\nstacymike\r\n");
        TextLineDemarcator demarcator = new TextLineDemarcator(is, 4);

        OffsetInfo offsetInfo = demarcator.nextOffsetInfo("foojhkj".getBytes());
        assertEquals(0, offsetInfo.getStartOffset());
        assertEquals(5, offsetInfo.getLength());
        assertEquals(1, offsetInfo.getCrlfLength());
        assertFalse(offsetInfo.isStartsWithMatch());

        offsetInfo = demarcator.nextOffsetInfo("foo".getBytes());
        assertEquals(5, offsetInfo.getStartOffset());
        assertEquals(4, offsetInfo.getLength());
        assertEquals(1, offsetInfo.getCrlfLength());
        assertFalse(offsetInfo.isStartsWithMatch());

        offsetInfo = demarcator.nextOffsetInfo("joe".getBytes());
        assertEquals(9, offsetInfo.getStartOffset());
        assertEquals(6, offsetInfo.getLength());
        assertEquals(2, offsetInfo.getCrlfLength());
        assertFalse(offsetInfo.isStartsWithMatch());

        offsetInfo = demarcator.nextOffsetInfo("stasy".getBytes());
        assertEquals(15, offsetInfo.getStartOffset());
        assertEquals(11, offsetInfo.getLength());
        assertEquals(2, offsetInfo.getCrlfLength());
        assertFalse(offsetInfo.isStartsWithMatch());
    }

    @Test
    public void startWithSomeMatches() throws Exception {
        InputStream is = stringToIs("oleg\rjoe\njack\r\nstacymike\r\n");
        TextLineDemarcator demarcator = new TextLineDemarcator(is, 7);

        OffsetInfo offsetInfo = demarcator.nextOffsetInfo("foojhkj".getBytes());
        assertEquals(0, offsetInfo.getStartOffset());
        assertEquals(5, offsetInfo.getLength());
        assertEquals(1, offsetInfo.getCrlfLength());
        assertFalse(offsetInfo.isStartsWithMatch());

        offsetInfo = demarcator.nextOffsetInfo("jo".getBytes());
        assertEquals(5, offsetInfo.getStartOffset());
        assertEquals(4, offsetInfo.getLength());
        assertEquals(1, offsetInfo.getCrlfLength());
        assertTrue(offsetInfo.isStartsWithMatch());

        offsetInfo = demarcator.nextOffsetInfo("joe".getBytes());
        assertEquals(9, offsetInfo.getStartOffset());
        assertEquals(6, offsetInfo.getLength());
        assertEquals(2, offsetInfo.getCrlfLength());
        assertFalse(offsetInfo.isStartsWithMatch());

        offsetInfo = demarcator.nextOffsetInfo("stacy".getBytes());
        assertEquals(15, offsetInfo.getStartOffset());
        assertEquals(11, offsetInfo.getLength());
        assertEquals(2, offsetInfo.getCrlfLength());
        assertTrue(offsetInfo.isStartsWithMatch());
    }

    @Test
    public void testOnBufferSplitNoTrailingDelimiter() throws IOException {
        final byte[] inputData = "Yes\nNo".getBytes(StandardCharsets.UTF_8);
        final ByteArrayInputStream is = new ByteArrayInputStream(inputData);
        final TextLineDemarcator demarcator = new TextLineDemarcator(is, 3);

        final OffsetInfo first = demarcator.nextOffsetInfo();
        final OffsetInfo second = demarcator.nextOffsetInfo();
        final OffsetInfo third = demarcator.nextOffsetInfo();
        assertNotNull(first);
        assertNotNull(second);
        assertNull(third);

        assertEquals(0, first.getStartOffset());
        assertEquals(4, first.getLength());
        assertEquals(1, first.getCrlfLength());

        assertEquals(4, second.getStartOffset());
        assertEquals(2, second.getLength());
        assertEquals(0, second.getCrlfLength());
    }

    private InputStream stringToIs(String data) {
        return new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
    }
}
