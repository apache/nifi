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
package org.apache.nifi.processors.standard.relp.frame;

import org.apache.nifi.processors.standard.relp.response.RELPResponse;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class TestRELPEncoder {

    @Test
    public void testEncodingWithData() throws IOException {
        final RELPFrame frame = new RELPFrame.Builder()
                .txnr(1)
                .command("rsp")
                .dataLength(5)
                .data("12345".getBytes(StandardCharsets.UTF_8))
                .build();

        final RELPEncoder encoder = new RELPEncoder(StandardCharsets.UTF_8);

        final byte[] result = encoder.encode(frame);

        final String expected = "1 rsp 5 12345\n";
        Assert.assertEquals(expected, new String(result, StandardCharsets.UTF_8));
    }

    @Test
    public void testEncodingNoData() throws IOException {
        final RELPFrame frame = new RELPFrame.Builder()
                .txnr(1)
                .command("rsp")
                .dataLength(0)
                .data(new byte[0])
                .build();

        final RELPEncoder encoder = new RELPEncoder(StandardCharsets.UTF_8);

        final byte[] result = encoder.encode(frame);

        final String expected = "1 rsp 0\n";
        Assert.assertEquals(expected, new String(result, StandardCharsets.UTF_8));
    }

    @Test
    public void testEncodingOpenResponse() {
        final String openFrameData = "relp_version=0\nrelp_software=librelp,1.2.7,http://librelp.adiscon.com\ncommands=syslog";
        final String openFrame = "1 open 85 " + openFrameData + "\n";
        System.out.println(openFrame);

        final RELPDecoder decoder = new RELPDecoder(StandardCharsets.UTF_8);
        final RELPEncoder encoder = new RELPEncoder(StandardCharsets.UTF_8);

        RELPFrame frame = null;
        for (byte b : openFrame.getBytes(StandardCharsets.UTF_8)) {
            if (decoder.process(b)) {
                frame = decoder.getFrame();
                break;
            }
        }

        Assert.assertNotNull(frame);

        final Map<String,String> offers = RELPResponse.parseOffers(frame.getData(), StandardCharsets.UTF_8);
        final RELPFrame responseFrame = RELPResponse.open(frame.getTxnr(), offers).toFrame(StandardCharsets.UTF_8);

        final byte[] response = encoder.encode(responseFrame);
        System.out.println(new String(response, StandardCharsets.UTF_8));
    }

}
