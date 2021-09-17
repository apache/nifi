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
import org.apache.nifi.processors.standard.relp.response.RELPChannelResponse;
import org.apache.nifi.processors.standard.relp.response.RELPResponse;
import org.apache.nifi.util.MockComponentLog;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

class RELPResponseEncoderTest {

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
    void testEncodeRELPResponse() throws IOException {

        final byte[] relpResponse = new RELPChannelResponse(new RELPEncoder(Charset.defaultCharset()), RELPResponse.ok(321L)).toByteArray();
//      final List<RELPResponse> frames = getRELPResponse(5);

        ByteBufOutputStream eventBytes = new ByteBufOutputStream(Unpooled.buffer(relpResponse.length));
        eventBytes.write(relpResponse);
        EmbeddedChannel channel = new EmbeddedChannel(new RELPResponseEncoder(Charset.defaultCharset()));

        assert(channel.writeOutbound(eventBytes));
        ByteBufOutputStream result = channel.readOutbound();
        assertEquals("321 rsp 6 200 OK\n", new String(result.buffer().array()));

//        assert(channel.writeOutbound(eventBytes.buffer()));
//        assertEquals(5, channel.outboundMessages().size());
//
//        RELPNettyEvent event = channel.readInbound();
//        assertEquals(RELPNettyEvent.class, event.getClass());
//        assertEquals(SYSLOG_FRAME_DATA, new String(event.getData(), StandardCharsets.UTF_8));
//
//        assertEquals(2, channel.outboundMessages());
    }

    private void sendFrames(final List<RELPFrame> frames, final OutputStream outputStream) throws IOException {
        RELPEncoder encoder = new RELPEncoder(Charset.defaultCharset());
        for (final RELPFrame frame : frames) {
            final byte[] encodedFrame = encoder.encode(frame);
            outputStream.write(encodedFrame);
            outputStream.flush();
        }
    }

    private List<RELPFrame> getRELPResponse(final int syslogFrames) {
        final List<RELPFrame> frames = new ArrayList<>();
        frames.add(OPEN_FRAME);

        for (int i = 0; i < syslogFrames; i++) {
            frames.add(SYSLOG_FRAME);
        }

        frames.add(CLOSE_FRAME);
        return frames;
    }

    /**  for (final RELPNettyEvent event : events) {
            eventSender.sendEvent(RELPResponse.ok(event.getTxnr()));
     }
    **/
}