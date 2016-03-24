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
package org.apache.nifi.processors.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.nio.charset.StandardCharsets;

import org.junit.Test;

public class SplittableMessageContextTest {

    @Test(expected = IllegalArgumentException.class)
    public void failNullEmptyTopic() {
        new SplittableMessageContext(null, null, null);
    }

    @Test
    public void validateFullSetting() {
        SplittableMessageContext ctx = new SplittableMessageContext("foo", "hello".getBytes(), "\n");
        ctx.setFailedSegments(1, 3, 6);
        assertEquals("\n", ctx.getDelimiterPattern());
        assertEquals("hello", new String(ctx.getKeyBytes(), StandardCharsets.UTF_8));
        assertEquals("foo", ctx.getTopicName());
        assertEquals("topic: 'foo'; delimiter: '\n'", ctx.toString());
    }


    @Test
    public void validateToString() {
        SplittableMessageContext ctx = new SplittableMessageContext("foo", null, null);
        assertEquals("topic: 'foo'; delimiter: '(\\W)\\Z'", ctx.toString());
    }

    @Test
    public void validateNoNPEandNoSideffectsOnSetsGets() {
        SplittableMessageContext ctx = new SplittableMessageContext("foo", null, null);
        ctx.setFailedSegments(null);
        assertNull(ctx.getFailedSegments());

        ctx.setFailedSegmentsAsByteArray(null);
        assertNull(ctx.getFailedSegments());

        assertEquals("(\\W)\\Z", ctx.getDelimiterPattern());;
        assertNull(ctx.getKeyBytes());
        assertNull(ctx.getKeyBytesAsString());
        assertEquals("foo", ctx.getTopicName());
    }
}
