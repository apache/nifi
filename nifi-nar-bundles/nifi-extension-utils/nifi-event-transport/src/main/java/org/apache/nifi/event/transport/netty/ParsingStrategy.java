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
package org.apache.nifi.event.transport.netty;

/**
 * Message Parsing Strategy
 */
public enum ParsingStrategy {
    /**
     * Messages are based only on stream end,
     * will error when receiving infinite stream
     */
    DISABLED,

    /**
     * Messages based on delimiter,
     * will error when receiving infinite stream without any line separators
     */
    SPLIT_ON_DELIMITER,

    /**
     * Message based on OctetCounting format defined in the RFC 6587
     * will error on invalid OctetCounting format
     */
    OCTET_COUNTING_STRICT,

    /** Use {@link io.netty.handler.codec.DelimiterBasedFrameDecoder} decoder when reading invalid OctetCounting format */
    OCTET_COUNTING_TOLERANT;

}
