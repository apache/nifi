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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class TestRELPDecoder {

    public static final String OPEN_FRAME_DATA = "relp_version=0\nrelp_software=librelp,1.2.7,http://librelp.adiscon.com\ncommands=syslog";
    public static final String OPEN_FRAME = "1 open 85 " + OPEN_FRAME_DATA + "\n";

    public static final String SYSLOG_FRAME_DATA = "this is a syslog message here";
    public static final String SYSLOG_FRAME = "2 syslog 29 " + SYSLOG_FRAME_DATA + "\n";

    public static final String CLOSE_FRAME = "3 close 0\n";

    private RELPDecoder decoder;

    @Before
    public void setup() {
        this.decoder = new RELPDecoder(StandardCharsets.UTF_8);
    }

    @Test
    public void testDecodeSingleFrame() throws RELPFrameException {
        final byte[] input = OPEN_FRAME.getBytes(StandardCharsets.UTF_8);

        RELPFrame frame = null;
        for (byte b : input) {
            if (decoder.process(b)) {
                frame = decoder.getFrame();
                break;
            }
        }

        Assert.assertNotNull(frame);
        Assert.assertEquals(1, frame.getTxnr());
        Assert.assertEquals("open", frame.getCommand());
        Assert.assertEquals(85, frame.getDataLength());

        Assert.assertNotNull(frame.getData());
        Assert.assertEquals(OPEN_FRAME_DATA, new String(frame.getData(), StandardCharsets.UTF_8));
    }

    @Test
    public void testDecodeMultipleCommands() throws RELPFrameException {
        final byte[] input = (OPEN_FRAME + SYSLOG_FRAME + CLOSE_FRAME).getBytes(StandardCharsets.UTF_8);

        List<RELPFrame> frames = new ArrayList<>();
        for (byte b : input) {
            if (decoder.process(b)) {
                frames.add(decoder.getFrame());
            }
        }

        Assert.assertEquals(3, frames.size());

        final RELPFrame frame1 = frames.get(0);
        Assert.assertNotNull(frame1);
        Assert.assertEquals(1, frame1.getTxnr());
        Assert.assertEquals("open", frame1.getCommand());
        Assert.assertEquals(85, frame1.getDataLength());

        Assert.assertNotNull(frame1.getData());
        Assert.assertEquals(OPEN_FRAME_DATA, new String(frame1.getData(), StandardCharsets.UTF_8));

        final RELPFrame frame2 = frames.get(1);
        Assert.assertNotNull(frame2);
        Assert.assertEquals(2, frame2.getTxnr());
        Assert.assertEquals("syslog", frame2.getCommand());
        Assert.assertEquals(29, frame2.getDataLength());

        Assert.assertNotNull(frame2.getData());
        Assert.assertEquals(SYSLOG_FRAME_DATA, new String(frame2.getData(), StandardCharsets.UTF_8));

        final RELPFrame frame3 = frames.get(2);
        Assert.assertNotNull(frame3);
        Assert.assertEquals(3, frame3.getTxnr());
        Assert.assertEquals("close", frame3.getCommand());
        Assert.assertEquals(0, frame3.getDataLength());
    }

    @Test
    public void testDecodeMultipleSyslogCommands() throws RELPFrameException {
        final String msg1 = "1 syslog 20 this is message 1234\n";
        final String msg2 = "2 syslog 22 this is message 456789\n";
        final String msg3 = "3 syslog 21 this is message ABCDE\n";
        final String msg = msg1 + msg2 + msg3;

        final byte[] input = msg.getBytes(StandardCharsets.UTF_8);

        List<RELPFrame> frames = new ArrayList<>();

        for (byte b : input) {
            if (decoder.process(b)) {
                frames.add(decoder.getFrame());
            }
        }

        Assert.assertEquals(3, frames.size());
    }

    @Test(expected = RELPFrameException.class)
    public void testBadDataShouldThrowException() throws RELPFrameException {
        final String msg = "NAN syslog 20 this is message 1234\n";
        final byte[] input = msg.getBytes(StandardCharsets.UTF_8);

        List<RELPFrame> frames = new ArrayList<>();

        for (byte b : input) {
            if (decoder.process(b)) {
                frames.add(decoder.getFrame());
            }
        }

        Assert.fail("Should have thrown exception");
    }
}
