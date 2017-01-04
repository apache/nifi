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
package org.apache.nifi.processors.beats.response;

import org.apache.nifi.processor.util.listen.response.ChannelResponse;
import org.apache.nifi.processors.beats.frame.BeatsFrame;
import org.apache.nifi.processors.beats.frame.BeatsEncoder;

/**
 * Creates a BeatsFrame for the provided response and returns the encoded frame.
 */
public class BeatsChannelResponse implements ChannelResponse {

    private final BeatsEncoder encoder;
    private final BeatsResponse response;

    public BeatsChannelResponse(final BeatsEncoder encoder, final BeatsResponse response) {
        this.encoder = encoder;
        this.response = response;
    }

    @Override
    public byte[] toByteArray() {
        final BeatsFrame frame = response.toFrame();
        return encoder.encode(frame);
    }

}
