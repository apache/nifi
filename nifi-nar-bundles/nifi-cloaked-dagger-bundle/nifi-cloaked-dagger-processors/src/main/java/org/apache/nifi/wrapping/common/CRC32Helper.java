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
package org.apache.nifi.wrapping.common;

//CHECKSTYLE.OFF: CustomImportOrderCheck
import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

//CHECKSTYLE.ON: CustomImportOrderCheck

/**
 * Helper for the CRC32 checksum.
 */
// CHECKSTYLE.OFF: AbbreviationAsWordInNameCheck
public class CRC32Helper implements CheckSumHelper {
    // CHECKSTYLE.ON: AbbreviationAsWordInNameCheck
    private final Checksum checksum;

    /**
     * Constructor to initialise local values.
     */
    public CRC32Helper() {
        checksum = new CRC32();
    }

    /*
     * (non-Javadoc)
     *
     * @see glib.app.fc.wrapping.common.CheckSumHelper#updateCheckSum(int)
     */
    @Override
    public void updateCheckSum(int src) {
        checksum.update(src);
    }

    /*
     * (non-Javadoc)
     *
     * @see glib.app.fc.wrapping.common.CheckSumHelper#updateCheckSum(byte[])
     */
    @Override
    public void updateCheckSum(byte[] bytes) {
        checksum.update(bytes, 0, bytes.length);
    }

    /*
     * (non-Javadoc)
     *
     * @see glib.app.fc.wrapping.common.CheckSumHelper#getCheckSum()
     */
    @Override
    public byte[] getCheckSum() {
        return ByteBuffer.allocate(8).putLong(checksum.getValue()).array();
    }
}
