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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

//CHECKSTYLE.ON: CustomImportOrderCheck

/**
 * Helper for the SHA256 checksum.
 */
// CHECKSTYLE.OFF: AbbreviationAsWordInNameCheck
public class SHA256Helper implements CheckSumHelper {
    // CHECKSTYLE.ON: AbbreviationAsWordInNameCheck
    private final MessageDigest md;

    /**
     * Constructor to initialise local values.
     *
     * @throws NoSuchAlgorithmException
     *             if the SHA-256 algorithm is not found.
     */
    public SHA256Helper() throws NoSuchAlgorithmException {
        md = MessageDigest.getInstance("SHA-256");
    }

    /*
     * (non-Javadoc)
     *
     * @see glib.app.fc.wrapping.common.CheckSumHelper#updateCheckSum(int)
     */
    @Override
    public void updateCheckSum(int src) {
        md.update((byte) src);
    }

    /*
     * (non-Javadoc)
     *
     * @see glib.app.fc.wrapping.common.CheckSumHelper#updateCheckSum(byte[])
     */
    @Override
    public void updateCheckSum(byte[] bytes) {
        md.update(bytes);
    }

    /*
     * (non-Javadoc)
     *
     * @see glib.app.fc.wrapping.common.CheckSumHelper#getCheckSum()
     */
    @Override
    public byte[] getCheckSum() {
        return md.digest();
    }
}
