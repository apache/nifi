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
import org.apache.nifi.processors.standard.relp.response.RELPChannelResponse;
import org.apache.nifi.processors.standard.relp.response.RELPResponse;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.Charset;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RELPResponseEncoderTest {

    @Test
    void testEncodeRELPResponse() throws IOException {
        final byte[] relpResponse = new RELPChannelResponse(new RELPEncoder(Charset.defaultCharset()), RELPResponse.ok(321L)).toByteArray();

        ByteBufOutputStream eventBytes = new ByteBufOutputStream(Unpooled.buffer(relpResponse.length));
        eventBytes.write(relpResponse);
        EmbeddedChannel channel = new EmbeddedChannel(new RELPResponseEncoder(Charset.defaultCharset()));

        assert(channel.writeOutbound(eventBytes));
        ByteBufOutputStream result = channel.readOutbound();
        assertEquals("321 rsp 6 200 OK\n", new String(result.buffer().array()));
    }
}