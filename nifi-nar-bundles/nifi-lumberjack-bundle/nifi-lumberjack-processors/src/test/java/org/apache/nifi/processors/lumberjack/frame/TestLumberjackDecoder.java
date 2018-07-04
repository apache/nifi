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
package org.apache.nifi.processors.lumberjack.frame;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import javax.xml.bind.DatatypeConverter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class TestLumberjackDecoder {

    // Because no encoder for type 43 was coded, added Static hex
    // representation of compressed data
    //
    private static final String singleFrameData = "3143000000aa785e4c8e4daac3300c8413c8cbfeddc017681da7b48540775df51245103936f54fb" +
        "04c4a6e5f6917d03020e91bc93c9ba669597faccefa80ec0fed72440dd1174833e819370c798d98aa0e79a10ae44e36972f94198b26886bc" +
        "0774422589024c865aaecff07f24c6e1b0c37fb6c2da18cdb4176834f72747c4152e6aa46330db7e9725707567db0240c93aace93e212464" +
        "95857f755e89e76e2d77e000000ffff010000ffff05b43bb8";
    private static final String multiFrameData = "3143000000d7785ec48fcf6ac4201087b3bbe9defb06be40ab669b1602bdf5d49728031957a97f82" +
        "232979fbaaa7c0924b2e018701f537f37df2ab699a53aea75cad321673ffe43a38e4e04c043f021f71461b26873e711bee9480f48b0af10fe28" +
        "89113b8c9e28f4322b82395413a50cafd79957c253d0b992faf4129c2f27c12e5af35be2cedbec133d9b34e0ee27db87db05596fd62f468079" +
        "6b421964fc9b032ac4dcb54d2575a28a3559df3413ae7be12edf6e9367c2e07f95ca4a848bb856e1b42ed61427d45da2df4f628f40f0000ffff" +
        "010000ffff35e0eff0";
    private static final String payload = "";

    private LumberjackDecoder decoder;

    @Before
    public void setup() {
        this.decoder = new LumberjackDecoder(StandardCharsets.UTF_8);
    }

    @Test
    public void testDecodeSingleFrame() {
        final byte[] input = DatatypeConverter.parseHexBinary(singleFrameData);

        List<LumberjackFrame> frames = null;
        LumberjackFrame frame = null;

        for (byte b : input) {
            if (decoder.process(b)) {
                frames = decoder.getFrames();
                break;
            }
        }

        frame = frames.get(frames.size() - 1);

        Assert.assertNotNull(frame);
        Assert.assertEquals(0x31, frame.getVersion());
        Assert.assertEquals(0x44, frame.getFrameType());
        Assert.assertEquals(1, frame.getSeqNumber());
        // Look for a predefined number of bytes for matching of the inner payload
        Assert.assertArrayEquals(DatatypeConverter.parseHexBinary("000000050000000466696c65000000"), Arrays.copyOfRange(frame.getPayload(), 0, 15));
    }

    @Test
    public void testDecodeMultipleFrame() {
        final byte[] input = DatatypeConverter.parseHexBinary(multiFrameData);

        List<LumberjackFrame> frames = null;
        LumberjackFrame frame = null;

        for (byte b : input) {
            if (decoder.process(b)) {
                frames = decoder.getFrames();
                break;
            }
        }

        frame = frames.get(1);

        Assert.assertNotNull(frame);
        Assert.assertEquals(0x31, frame.getVersion());
        Assert.assertEquals(0x44, frame.getFrameType());
        // Load the second frame therefore seqNumber = 2
        Assert.assertEquals(2, frame.getSeqNumber());
        // Look for a predefined number of bytes for matching of the inner payload
        Assert.assertArrayEquals(DatatypeConverter.parseHexBinary("000000050000000466696c65000000"), Arrays.copyOfRange(frame.getPayload(), 0, 15));
    }
}
