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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.nifi.wrapping.unwrapper.Unwrapper;
import org.apache.nifi.wrapping.wrapper.Wrapper;
import org.apache.nifi.wrapping.wrapper.WrapperFactory;

/**
 * Command line interface to Wrapping tools.
 */
public class WrappingTool {

    /**
     * Static method to wrap a given file and produce output to a file.
     *
     * @param fileIn
     *            - name of the file to be read and wrapped.
     * @param fileOut
     *            - name of the file to be created with wrapped content.
     * @param technique
     *            - technique to use for wrapping.
     * @param maskLength
     *            - length of mask to use.
     * @param headerChecksum
     *            - header checksum to use.
     * @param bodyChecksum
     *            - body checksum to use.
     * @throws IOException
     *             for any problems with reading or writing files.
     */
    public static void wrap(final String fileIn, final String fileOut, final String technique,
        final String maskLength, final String headerChecksum, final String bodyChecksum) throws IOException {
        final File fileToUnwrap = new File(fileIn);

        try (BufferedInputStream bufferedIn = new BufferedInputStream(new FileInputStream(fileToUnwrap));
                        BufferedOutputStream bufferedOut = new BufferedOutputStream(new FileOutputStream(fileOut))) {

            final WrapperFactory.MaskLength maskLen;
            if ("128".equals(maskLength)) {
                maskLen = WrapperFactory.MaskLength.MASK_128_BIT;
            } else if ("8".equals(maskLength)) {
                maskLen = WrapperFactory.MaskLength.MASK_8_BIT;
            } else if ("16".equals(maskLength)) {
                maskLen = WrapperFactory.MaskLength.MASK_16_BIT;
            } else if ("32".equals(maskLength)) {
                maskLen = WrapperFactory.MaskLength.MASK_32_BIT;
            } else if ("192".equals(maskLength)) {
                maskLen = WrapperFactory.MaskLength.MASK_192_BIT;
            } else if ("256".equals(maskLength)) {
                maskLen = WrapperFactory.MaskLength.MASK_256_BIT;
            } else if ("64".equals(maskLength)) {
                maskLen = WrapperFactory.MaskLength.MASK_64_BIT;
            } else {
                maskLen = null;
            }

            final WrapperFactory.TechniqueType tech;
            if ("Technique1".equals(technique)) {
                tech = WrapperFactory.TechniqueType.TECHNIQUE_1;
            } else if ("Technique2".equals(technique)) {
                tech = WrapperFactory.TechniqueType.TECHNIQUE_2;
            } else {
                tech = null;
            }

            final WrapperFactory.CheckSum hcs;
            if ("CRC32".equals(headerChecksum)) {
                hcs = WrapperFactory.CheckSum.CRC_32_CHECKSUM;
            } else if ("SHA256".equals(headerChecksum)) {
                hcs = WrapperFactory.CheckSum.SHA_256_CHECKSUM;
            } else {
                hcs = WrapperFactory.CheckSum.NO_CHECKSUM;
            }

            final WrapperFactory.CheckSum bcs;
            if ("CRC32".equals(bodyChecksum)) {
                bcs = WrapperFactory.CheckSum.CRC_32_CHECKSUM;
            } else if ("SHA256".equals(bodyChecksum)) {
                bcs = WrapperFactory.CheckSum.SHA_256_CHECKSUM;
            } else {
                bcs = WrapperFactory.CheckSum.NO_CHECKSUM;
            }

            final WrapperFactory wrapperFactory = new WrapperFactory();
            final Wrapper wrapper = wrapperFactory.getWrapper(bufferedOut, maskLen, tech, hcs, bcs);

            final byte[] buffer = new byte[8192];
            int got = bufferedIn.read(buffer);
            while (got > 0) {
                if (got != buffer.length) {
                    wrapper.write(buffer, 0, got);
                } else {
                    wrapper.write(buffer);
                }
                got = bufferedIn.read(buffer);
            }

            wrapper.close();
        } // End Try-Release
    }

    /**
     * Static method to unwrap a given file and produce output to a file.
     *
     * @param fileIn
     *            - name of the file to be read and unwrapped.
     * @param fileOut
     *            - name of the file to be created with unwrapped content.
     * @throws IOException
     *             for any problems with reading or writing files.
     */
    public static void unwrap(String fileIn, String fileOut) throws IOException {
        try (BufferedInputStream bufferedIn = new BufferedInputStream(new FileInputStream(fileIn));
                        BufferedOutputStream bufferedOut = new BufferedOutputStream(new FileOutputStream(fileOut))) {

            // Set up the unwrapper.
            final Unwrapper unwrapper = new Unwrapper(bufferedIn, bufferedOut);

            // Unwrap the data.
            final byte[] buffer = new byte[4096];

            int got = unwrapper.read(buffer);
            while (got != -1) {
                got = unwrapper.read(buffer);
            }
            unwrapper.close();
        } // End Try-Release
    }

    /**
     * Main method that can be run.
     *
     * @param args
     *            - array of runtime arguments - File In, File Out, [wrap|unwrap], wrap params.
     * @throws IOException
     *             - any problems.
     */
    public static void main(String[] args) throws IOException {
        if (args[2].equals("wrap")) {
            wrap(args[0], args[1], args[3], args[4], args[5], args[6]);

        }
        if (args[2].equals("unwrap")) {
            unwrap(args[0], args[1]);
        }

    }
}
