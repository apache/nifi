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
package org.apache.nifi.processors.standard.relp.response;

import org.apache.nifi.processors.standard.relp.frame.RELPFrame;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRELPResponse {

    @Test
    public void testResponseToFrame() {
        final long txnr = 123456789;
        final int code = RELPResponse.OK;
        final String message = "this is a message";
        final String data = "this is some data";

        final RELPResponse response = new RELPResponse(txnr, code, message, data);

        final RELPFrame frame = response.toFrame(StandardCharsets.UTF_8);
        assertEquals(txnr, frame.getTxnr());
        assertEquals(RELPResponse.RSP_CMD, frame.getCommand());

        final String result = new String(frame.getData(), StandardCharsets.UTF_8);
        final String expected = code + " " + message + "\n" + data;
        assertEquals(expected, result);
        assertEquals(expected.length(), frame.getDataLength());
    }

    @Test
    public void testResponseToFrameNoMessage() {
        final long txnr = 123456789;
        final int code = RELPResponse.OK;
        final String data = "this is some data";

        final RELPResponse response = new RELPResponse(txnr, code, null, data);

        final RELPFrame frame = response.toFrame(StandardCharsets.UTF_8);
        assertEquals(txnr, frame.getTxnr());
        assertEquals(RELPResponse.RSP_CMD, frame.getCommand());

        final String result = new String(frame.getData(), StandardCharsets.UTF_8);
        final String expected = code + "\n" + data;
        assertEquals(expected, result);
        assertEquals(expected.length(), frame.getDataLength());
    }

    @Test
    public void testResponseToFrameNoData() throws IOException {
        final long txnr = 123456789;
        final int code = RELPResponse.OK;
        final String message = "this is a message";

        final RELPResponse response = new RELPResponse(txnr, code, message, null);

        final RELPFrame frame = response.toFrame(StandardCharsets.UTF_8);
        assertEquals(txnr, frame.getTxnr());
        assertEquals(RELPResponse.RSP_CMD, frame.getCommand());

        final String result = new String(frame.getData(), StandardCharsets.UTF_8);
        final String expected = code + " " + message;
        assertEquals(expected, result);
        assertEquals(expected.length(), frame.getDataLength());
    }

    @Test
    public void testResponseToFrameNoDataNoMessage() throws IOException {
        final long txnr = 123456789;
        final int code = RELPResponse.OK;

        final RELPResponse response = new RELPResponse(txnr, code);

        final RELPFrame frame = response.toFrame(StandardCharsets.UTF_8);
        assertEquals(txnr, frame.getTxnr());
        assertEquals(RELPResponse.RSP_CMD, frame.getCommand());

        final String result = new String(frame.getData(), StandardCharsets.UTF_8);
        final String expected = code + "";
        assertEquals(expected, result);
        assertEquals(expected.length(), frame.getDataLength());
    }

    @Test
    public void testCreateOpenResponse() {
        final long txnr = 123456789;

        final Map<String,String> offers = new HashMap<>();
        offers.put("key1", "val1");
        offers.put("key2", "val2");

        final RELPResponse openResponse = RELPResponse.open(txnr, offers);

        final RELPFrame frame = openResponse.toFrame(StandardCharsets.UTF_8);
        assertEquals(txnr, frame.getTxnr());
        assertEquals(RELPResponse.RSP_CMD, frame.getCommand());

        final String result = new String(frame.getData(), StandardCharsets.UTF_8);
        final String expected1 = RELPResponse.OK + " OK\n" + "key1=val1\nkey2=val2";
        final String expected2 = RELPResponse.OK + " OK\n" + "key2=val2\nkey1=val1";
        assertTrue(result.equals(expected1) || result.equals(expected2));
        assertEquals(expected1.length(), frame.getDataLength());
    }

    @Test
    public void testCreateOpenResponseNoOffers() {
        final long txnr = 123456789;
        final Map<String,String> offers = new HashMap<>();

        final RELPResponse openResponse = RELPResponse.open(txnr, offers);

        final RELPFrame frame = openResponse.toFrame(StandardCharsets.UTF_8);
        assertEquals(txnr, frame.getTxnr());
        assertEquals(RELPResponse.RSP_CMD, frame.getCommand());

        final String result = new String(frame.getData(), StandardCharsets.UTF_8);
        final String expected = RELPResponse.OK + " OK\n";
        assertEquals(expected, result);
        assertEquals(expected.length(), frame.getDataLength());
    }

    @Test
    public void testCreateOkResponse() {
        final long txnr = 123456789;
        final RELPResponse openResponse = RELPResponse.ok(txnr);

        final RELPFrame frame = openResponse.toFrame(StandardCharsets.UTF_8);
        assertEquals(txnr, frame.getTxnr());
        assertEquals(RELPResponse.RSP_CMD, frame.getCommand());

        final String result = new String(frame.getData(), StandardCharsets.UTF_8);
        final String expected = RELPResponse.OK + " OK";
        assertEquals(expected, result);
        assertEquals(expected.length(), frame.getDataLength());
    }

    @Test
    public void testCreateErrorResponse() {
        final long txnr = 123456789;
        final RELPResponse openResponse = RELPResponse.error(txnr);

        final RELPFrame frame = openResponse.toFrame(StandardCharsets.UTF_8);
        assertEquals(txnr, frame.getTxnr());
        assertEquals(RELPResponse.RSP_CMD, frame.getCommand());

        final String result = new String(frame.getData(), StandardCharsets.UTF_8);
        final String expected = RELPResponse.ERROR + " ERROR";
        assertEquals(expected, result);
        assertEquals(expected.length(), frame.getDataLength());
    }
}
