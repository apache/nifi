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
package org.apache.nifi.processors.aws.kinesis;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProvenanceTransitUriFormatTest {

    @ParameterizedTest
    @CsvSource("""
            streamA,shardId123,kinesis:stream/streamA/shardId123
            stream-name-b,shardId-000000013,kinesis:stream/stream-name-b/shardId-000000013
            kinesis,shardId-00000001,kinesis:stream/kinesis/shardId-00000001
            """)
    void toTransitUri(final String streamName, final String shardId, final String expectedTransitUri) {
        final String transitUri = ProvenanceTransitUriFormat.toTransitUri(streamName, shardId);
        assertEquals(expectedTransitUri, transitUri);
    }
}
