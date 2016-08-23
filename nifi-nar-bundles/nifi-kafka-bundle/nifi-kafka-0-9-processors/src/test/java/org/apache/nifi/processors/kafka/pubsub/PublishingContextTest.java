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
package org.apache.nifi.processors.kafka.pubsub;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.junit.Test;

public class PublishingContextTest {

    @Test
    public void failInvalidConstructorArgs() {
        try {
            new PublishingContext(null, null);
            fail();
        } catch (IllegalArgumentException e) {
            // success
        }
        try {
            new PublishingContext(mock(InputStream.class), null);
            fail();
        } catch (IllegalArgumentException e) {
            // success
        }

        try {
            new PublishingContext(mock(InputStream.class), "");
            fail();
        } catch (IllegalArgumentException e) {
            // success
        }

        try {
            new PublishingContext(mock(InputStream.class), "mytopic", -3);
            fail();
        } catch (IllegalArgumentException e) {
            // success
        }
    }

    @Test
    public void validateFullSetting() {
        PublishingContext publishingContext = new PublishingContext(mock(InputStream.class), "topic", 3);
        publishingContext.setDelimiterBytes("delimiter".getBytes(StandardCharsets.UTF_8));
        publishingContext.setKeyBytes("key".getBytes(StandardCharsets.UTF_8));

        assertEquals("delimiter", new String(publishingContext.getDelimiterBytes(), StandardCharsets.UTF_8));
        assertEquals("key", new String(publishingContext.getKeyBytes(), StandardCharsets.UTF_8));
        assertEquals("topic", publishingContext.getTopic());
        assertEquals("topic: 'topic'; delimiter: 'delimiter'", publishingContext.toString());
    }

    @Test
    public void validateOnlyOnceSetPerInstance() {
        PublishingContext publishingContext = new PublishingContext(mock(InputStream.class), "topic");
        publishingContext.setKeyBytes(new byte[]{0});
        try {
            publishingContext.setKeyBytes(new byte[]{0});
            fail();
        } catch (IllegalArgumentException e) {
            // success
        }

        publishingContext.setDelimiterBytes(new byte[]{0});
        try {
            publishingContext.setDelimiterBytes(new byte[]{0});
            fail();
        } catch (IllegalArgumentException e) {
            // success
        }

        publishingContext.setMaxRequestSize(1024);
        try {
            publishingContext.setMaxRequestSize(1024);
            fail();
        } catch (IllegalArgumentException e) {
            // success
        }

        try {
            publishingContext.setMaxRequestSize(-10);
            fail();
        } catch (IllegalArgumentException e) {
            // success
        }
    }
}
