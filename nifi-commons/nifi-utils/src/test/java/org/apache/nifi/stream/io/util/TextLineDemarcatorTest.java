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
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

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

    @Test
    public void singleCR() {
        InputStream is = stringToIs("\r");
        TextLineDemarcator demarcator = new TextLineDemarcator(is);
        long[] offsetInfo = demarcator.nextOffsetInfo();
        assertEquals(0, offsetInfo[0]); // offset
        assertEquals(1, offsetInfo[1]); // length
        assertEquals(1, offsetInfo[2]); // CR or CRLF - values can only be 1 or 2
        assertEquals(1, offsetInfo[3]); // EOF
    }

    @Test
    public void singleLF() {
        InputStream is = stringToIs("\n");
        TextLineDemarcator demarcator = new TextLineDemarcator(is);
        long[] offsetInfo = demarcator.nextOffsetInfo();
        assertEquals(0, offsetInfo[0]); // offset
        assertEquals(1, offsetInfo[1]); // length
        assertEquals(1, offsetInfo[2]); // CR or CRLF - values can only be 1 or 2
        assertEquals(1, offsetInfo[3]); // EOF
    }

    @Test
    // essentially validates the internal 'isEol()' operation to ensure it will perform read-ahead
    public void crlfWhereLFdoesNotFitInInitialBuffer() throws Exception {
        InputStream is = stringToIs("oleg\r\njoe");
        TextLineDemarcator demarcator = new TextLineDemarcator(is, 5);
        long[] offsetInfo = demarcator.nextOffsetInfo();
        assertEquals(0, offsetInfo[0]);
        assertEquals(6, offsetInfo[1]);
        assertEquals(2, offsetInfo[2]);
        assertEquals(1, offsetInfo[3]);

        offsetInfo = demarcator.nextOffsetInfo();
        assertEquals(6, offsetInfo[0]);
        assertEquals(3, offsetInfo[1]);
        assertEquals(0, offsetInfo[2]);
        assertEquals(1, offsetInfo[3]);
    }

    @Test
    public void mixedCRLF() throws Exception {
        InputStream is = stringToIs("oleg\rjoe\njack\r\nstacymike\r\n");
        TextLineDemarcator demarcator = new TextLineDemarcator(is, 4);
        long[] offsetInfo = demarcator.nextOffsetInfo();
        assertEquals(0, offsetInfo[0]);
        assertEquals(5, offsetInfo[1]);
        assertEquals(1, offsetInfo[2]);
        assertEquals(1, offsetInfo[3]);

        offsetInfo = demarcator.nextOffsetInfo();
        assertEquals(5, offsetInfo[0]);
        assertEquals(4, offsetInfo[1]);
        assertEquals(1, offsetInfo[2]);
        assertEquals(1, offsetInfo[3]);

        offsetInfo = demarcator.nextOffsetInfo();
        assertEquals(9, offsetInfo[0]);
        assertEquals(6, offsetInfo[1]);
        assertEquals(2, offsetInfo[2]);
        assertEquals(1, offsetInfo[3]);

        offsetInfo = demarcator.nextOffsetInfo();
        assertEquals(15, offsetInfo[0]);
        assertEquals(11, offsetInfo[1]);
        assertEquals(2, offsetInfo[2]);
        assertEquals(1, offsetInfo[3]);
    }

    @Test
    public void consecutiveAndMixedCRLF() throws Exception {
        InputStream is = stringToIs("oleg\r\r\njoe\n\n\rjack\n\r\nstacymike\r\n\n\n\r");
        TextLineDemarcator demarcator = new TextLineDemarcator(is, 4);

        long[] offsetInfo = demarcator.nextOffsetInfo(); // oleg\r
        assertEquals(5, offsetInfo[1]);
        assertEquals(1, offsetInfo[2]);

        offsetInfo = demarcator.nextOffsetInfo();        // \r\n
        assertEquals(2, offsetInfo[1]);
        assertEquals(2, offsetInfo[2]);

        offsetInfo = demarcator.nextOffsetInfo();        // joe\n
        assertEquals(4, offsetInfo[1]);
        assertEquals(1, offsetInfo[2]);

        offsetInfo = demarcator.nextOffsetInfo();        // \n
        assertEquals(1, offsetInfo[1]);
        assertEquals(1, offsetInfo[2]);

        offsetInfo = demarcator.nextOffsetInfo();        // \r
        assertEquals(1, offsetInfo[1]);
        assertEquals(1, offsetInfo[2]);

        offsetInfo = demarcator.nextOffsetInfo();        // jack\n
        assertEquals(5, offsetInfo[1]);
        assertEquals(1, offsetInfo[2]);

        offsetInfo = demarcator.nextOffsetInfo();        // \r\n
        assertEquals(2, offsetInfo[1]);
        assertEquals(2, offsetInfo[2]);

        offsetInfo = demarcator.nextOffsetInfo();        // stacymike\r\n
        assertEquals(11, offsetInfo[1]);
        assertEquals(2, offsetInfo[2]);

        offsetInfo = demarcator.nextOffsetInfo();        // \n
        assertEquals(1, offsetInfo[1]);
        assertEquals(1, offsetInfo[2]);

        offsetInfo = demarcator.nextOffsetInfo();        // \n
        assertEquals(1, offsetInfo[1]);
        assertEquals(1, offsetInfo[2]);

        offsetInfo = demarcator.nextOffsetInfo();        // \r
        assertEquals(1, offsetInfo[1]);
        assertEquals(1, offsetInfo[2]);
    }

    @Test
    public void startWithNoMatchOnWholeStream() throws Exception {
        InputStream is = stringToIs("oleg\rjoe\njack\r\nstacymike\r\n");
        TextLineDemarcator demarcator = new TextLineDemarcator(is, 4);

        long[] offsetInfo = demarcator.nextOffsetInfo("foojhkj".getBytes());
        assertEquals(0, offsetInfo[0]);
        assertEquals(5, offsetInfo[1]);
        assertEquals(1, offsetInfo[2]);
        assertEquals(0, offsetInfo[3]);

        offsetInfo = demarcator.nextOffsetInfo("foo".getBytes());
        assertEquals(5, offsetInfo[0]);
        assertEquals(4, offsetInfo[1]);
        assertEquals(1, offsetInfo[2]);
        assertEquals(0, offsetInfo[3]);

        offsetInfo = demarcator.nextOffsetInfo("joe".getBytes());
        assertEquals(9, offsetInfo[0]);
        assertEquals(6, offsetInfo[1]);
        assertEquals(2, offsetInfo[2]);
        assertEquals(0, offsetInfo[3]);

        offsetInfo = demarcator.nextOffsetInfo("stasy".getBytes());
        assertEquals(15, offsetInfo[0]);
        assertEquals(11, offsetInfo[1]);
        assertEquals(2, offsetInfo[2]);
        assertEquals(0, offsetInfo[3]);
    }

    @Test
    public void startWithSomeMatches() throws Exception {
        InputStream is = stringToIs("oleg\rjoe\njack\r\nstacymike\r\n");
        TextLineDemarcator demarcator = new TextLineDemarcator(is, 7);

        long[] offsetInfo = demarcator.nextOffsetInfo("foojhkj".getBytes());
        assertEquals(0, offsetInfo[0]);
        assertEquals(5, offsetInfo[1]);
        assertEquals(1, offsetInfo[2]);
        assertEquals(0, offsetInfo[3]);

        offsetInfo = demarcator.nextOffsetInfo("jo".getBytes());
        assertEquals(5, offsetInfo[0]);
        assertEquals(4, offsetInfo[1]);
        assertEquals(1, offsetInfo[2]);
        assertEquals(1, offsetInfo[3]);

        offsetInfo = demarcator.nextOffsetInfo("joe".getBytes());
        assertEquals(9, offsetInfo[0]);
        assertEquals(6, offsetInfo[1]);
        assertEquals(2, offsetInfo[2]);
        assertEquals(0, offsetInfo[3]);

        offsetInfo = demarcator.nextOffsetInfo("stacy".getBytes());
        assertEquals(15, offsetInfo[0]);
        assertEquals(11, offsetInfo[1]);
        assertEquals(2, offsetInfo[2]);
        assertEquals(1, offsetInfo[3]);
    }

    private InputStream stringToIs(String data) {
        return new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
    }
}
