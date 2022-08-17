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

import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.standard.relp.event.RELPMessage;
import org.apache.nifi.util.MockComponentLog;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RELPFrameDecoderTest {

    final ComponentLog logger = new MockComponentLog(this.getClass().getSimpleName(), this);

    public static final String OPEN_FRAME_DATA = "relp_version=0\nrelp_software=librelp,1.2.7,http://librelp.adiscon.com\ncommands=syslog";
    public static final String SYSLOG_FRAME_DATA = "this is a syslog message here";

    static final RELPFrame OPEN_FRAME = new RELPFrame.Builder()
            .txnr(1)
            .command("open")
            .dataLength(OPEN_FRAME_DATA.length())
            .data(OPEN_FRAME_DATA.getBytes(StandardCharsets.UTF_8))
            .build();

    static final RELPFrame SYSLOG_FRAME = new RELPFrame.Builder()
            .txnr(2)
            .command("syslog")
            .dataLength(SYSLOG_FRAME_DATA.length())
            .data(SYSLOG_FRAME_DATA.getBytes(StandardCharsets.UTF_8))
            .build();

    static final RELPFrame CLOSE_FRAME = new RELPFrame.Builder()
            .txnr(3)
            .command("close")
            .dataLength(0)
            .data(new byte[0])
            .build();

    @Test
    void testDecodeRELPEvents() throws IOException {
        final List<RELPFrame> frames = getFrames(5);
        ByteBufOutputStream eventBytes = new ByteBufOutputStream(Unpooled.buffer());
        sendFrames(frames, eventBytes);
        EmbeddedChannel channel = new EmbeddedChannel(new RELPFrameDecoder(logger, StandardCharsets.UTF_8));

        assert(channel.writeInbound(eventBytes.buffer()));
        assertEquals(5, channel.inboundMessages().size());

        RELPMessage event = channel.readInbound();
        assertEquals(RELPMessage.class, event.getClass());
        assertEquals(SYSLOG_FRAME_DATA, new String(event.getMessage(), StandardCharsets.UTF_8));
        assertEquals(2, channel.outboundMessages().size());
    }

    private void sendFrames(final List<RELPFrame> frames, final OutputStream outputStream) throws IOException {
        RELPEncoder encoder = new RELPEncoder(StandardCharsets.UTF_8);
        for (final RELPFrame frame : frames) {
            final byte[] encodedFrame = encoder.encode(frame);
            outputStream.write(encodedFrame);
            outputStream.flush();
        }
    }

    private List<RELPFrame> getFrames(final int syslogFrames) {
        final List<RELPFrame> frames = new ArrayList<>();
        frames.add(OPEN_FRAME);

        for (int i = 0; i < syslogFrames; i++) {
            frames.add(SYSLOG_FRAME);
        }

        frames.add(CLOSE_FRAME);
        return frames;
    }
}