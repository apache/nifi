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

import org.junit.Test;

public class TestRELPFrame {

    @Test(expected = RELPFrameException.class)
    public void testInvalidTxnr() {
        new RELPFrame.Builder().command("command").dataLength(5).data(new byte[5]).build();
    }

    @Test(expected = RELPFrameException.class)
    public void testInvalidCommand() {
        new RELPFrame.Builder().txnr(1).dataLength(5).data(new byte[5]).build();
    }

    @Test(expected = RELPFrameException.class)
    public void testBlankCommand() {
        new RELPFrame.Builder().txnr(1).command("  ").dataLength(5).data(new byte[5]).build();
    }

    @Test(expected = RELPFrameException.class)
    public void testInvalidDataLength() {
        new RELPFrame.Builder().txnr(1).command("command").data(new byte[5]).build();
    }

    @Test(expected = RELPFrameException.class)
    public void testInvalidData() {
        new RELPFrame.Builder().txnr(1).command("command").dataLength(5).data(null).build();
    }
}
