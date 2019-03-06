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

//Dev use only.

package org.apache.nifi.wrapping.wrapper;

import static org.junit.Assert.assertEquals;

import org.apache.nifi.wrapping.wrapper.Header;
import org.apache.nifi.wrapping.wrapper.Technique;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
//import java.io.FileNotFoundException;
//import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

//import java.util.BitSet;

public class HeaderTest {

    private static final String TEST_GIF_FILENAME = "src/test/resources/inputfile.gif";

    /**
     * Input file bytes.
     */
    byte[] inputFile;

    /**
     * 128 bit test mask.
     */
    byte[] mask = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00 };

    /**
     * Read in a test file for use by the test methods.
     *
     * @throws IOException upon failure to access test files
     */
    @Before
    public void setUp() throws IOException {
        // Get a file from the resources directory to read in.
        String inputFileLocation = TEST_GIF_FILENAME;
        // Read the mask into memory.
        File file = new File(inputFileLocation);
        InputStream is = new FileInputStream(file);
        long length = file.length();
        inputFile = new byte[(int) length];
        int offset = 0;
        int numRead = 0;
        while ((offset < inputFile.length)
                        && ((numRead = is.read(inputFile, offset, inputFile.length - offset)) >= 0)) {
            offset += numRead;
        }
        is.close();
    }

    /**
     * Check the header is built correctly for Technique 1 with no checksums specified.
     */
    @Test
    public void testTechnique1NoChecksums() {
        // Expected result for this test.
        // originally expectedResult = { -47, -33, 95, -1, 0, 1, 0, 0, 0, 0, 0, 56, 0, 0, 0, 1, 1, 0, 0, 16, 0,
        byte[] expectedResult = new byte[] { -47, -33, 95, -1, 0, 1, 0, 0, 0, 0, 0, 56, 0, 0, 0, 1, 0, 4, 0, 16, 0,
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        -1, 95, -33, -47 };

        // To create a header, we need to provide three techniques - the encapsulation, and the two
        // checksums (empty if not required).

        // Use technique 1 for this.
        Technique encapsulation = new Technique(Technique.TECHNIQUE_1, mask);

        // Pass values to header, remembering null for the two checksums that we aren't using.
        Header header = new Header(encapsulation, null, null);

        // Get the constructed header in the form of a byte[] ready for output.
        byte[] headerBytes = header.getHeaderBytes();

        // Check the length.
        assertEquals(expectedResult.length, headerBytes.length);

        // Check that each value is correct.
        for (int i = 0; i < headerBytes.length; i++) {
            int a = headerBytes[i];
            int b = expectedResult[i];
            assertEquals(b, a);
        }

        // Now check that the header produced is of the length stated.
        ByteBuffer b = ByteBuffer
                        .wrap(new byte[] { headerBytes[8], headerBytes[9], headerBytes[10], headerBytes[11] });
        assertEquals(headerBytes.length, b.getInt());
    }

    @Test
    public void testTechnique1CRC() {
        // Expected result for this test.
        // originally expectedResult = { -47, -33, 95, -1, 0, 1, 0, 0, 0, 0, 0, 60, 0, 0, 0, 1, 1, 0, 0, 16, 0,
        byte[] expectedResult = new byte[] { -47, -33, 95, -1, 0, 1, 0, 0, 0, 0, 0, 60, 0, 0, 0, 1, 0, 4, 0, 16, 0,
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0,
                        -83, 8, 119, -56, -1, 95, -33, -47 };
        // expectedResult -80, -30, -16, -1, -1, 95, -33, -47 };

        // To create a header, we need to provide three techniques - the encapsulation, and the two
        // checksums (empty if not required).

        // Use technique 1 for this.
        Technique encapsulation = new Technique(Technique.TECHNIQUE_1, mask);

        // Pass values to header.
        Header header = new Header(encapsulation, "CRC32", "CRC32");

        // Get the constructed header in the form of a byte[] ready for output.
        byte[] headerBytes = header.getHeaderBytes();

        // Check the length.
        assertEquals("The length of the returned header was not as expected.", expectedResult.length,
                        headerBytes.length);

        // Check that each value is correct.
        for (int i = 0; i < headerBytes.length; i++) {
            int a = headerBytes[i];
            int b = expectedResult[i];
            assertEquals("One of the bytes within the processed header was not as expected.", b, a);
        }

        // Now check that the header produced is of the length stated.
        ByteBuffer b = ByteBuffer
                        .wrap(new byte[] { headerBytes[8], headerBytes[9], headerBytes[10], headerBytes[11] });
        assertEquals("The header length differed from the value within the header itself.", headerBytes.length,
                        b.getInt());
    }

    @Test
    public void testTechnique1SHA() {
        // Expected result for this test.
        // originally expectedResult = { -47, -33, 95, -1, 0, 1, 0, 0, 0, 0, 0, 88, 0, 0, 0, 1, 1, 0, 0, 16, 0,
        byte[] expectedResult = new byte[] { -47, -33, 95, -1, 0, 1, 0, 0, 0, 0, 0, 88, 0, 0, 0, 1, 0, 4, 0, 16, 0,
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0,
                        91, -124, -37, 40, 26, -93, -95, -42, -43, -71, 34, 18, 58, 73, -113, -75, -44, -91, -84,
                        // -34, -71, -65, 110, -18, 55, -85, 10, 14, -119, -44, 41, -103, -74, 88, -83, 120, -94, 45,
                        79, -67, 123, 37, -35, -109, 85, 124, 79, -120, 87, 88, 48, -1, 95, -33, -47 };
        // expectedResult 119, 38, -60, 66, -36, 27, 116, 115, 85, 125, -11, -103, -104, -1, 95, -33, -47 };

        // To create a header, we need to provide three techniques - the encapsulation, and the two
        // checksums (empty if not required).

        // Use technique 1 for this.
        Technique encapsulation = new Technique(Technique.TECHNIQUE_1, mask);

        // Pass values to header.
        Header header = new Header(encapsulation, "SHA256", "SHA256");

        // Get the constructed header in the form of a byte[] ready for output.
        byte[] headerBytes = header.getHeaderBytes();

        // Check the length.
        assertEquals("The length of the returned header was not as expected.", expectedResult.length,
                        headerBytes.length);

        // Check that each value is correct.
        for (int i = 0; i < headerBytes.length; i++) {
            int a = headerBytes[i];
            int b = expectedResult[i];
            assertEquals("One of the bytes within the processed header was not as expected.", b, a);
        }

        // Now check that the header produced is of the length stated.
        ByteBuffer b = ByteBuffer
                        .wrap(new byte[] { headerBytes[8], headerBytes[9], headerBytes[10], headerBytes[11] });
        assertEquals("The header length differed from the value within the header itself.", headerBytes.length,
                        b.getInt());
    }

    /**
     * Check the header is built correctly for Technique 2 with no checksums specified.
     */
    @Test
    public void testTechnique2NoChecksums() {
        // Expected result for this test.
        // originally expectedResult = { -47, -33, 95, -1, 0, 1, 0, 0, 0, 0, 0, 56, 0, 0, 0, 2, 1, 0, 0, 16, 0,
        byte[] expectedResult = new byte[] { -47, -33, 95, -1, 0, 1, 0, 0, 0, 0, 0, 56, 0, 0, 0, 2, 0, 4, 0, 16, 0,
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        -1, 95, -33, -47 };

        // To create a header, we need to provide three techniques - the encapsulation, and the two
        // checksums (empty if not required).

        // Use technique 2 for this.
        Technique encapsulation = new Technique(Technique.TECHNIQUE_2, mask);

        // Pass values to header, remembering null for the two checksums that we aren't using.
        Header header = new Header(encapsulation, null, null);

        // Get the constructed header in the form of a byte[] ready for output.
        byte[] headerBytes = header.getHeaderBytes();

        // Check the length.
        assertEquals(expectedResult.length, headerBytes.length);

        // Check that each value is correct.
        for (int i = 0; i < headerBytes.length; i++) {
            int a = headerBytes[i];
            int b = expectedResult[i];
            assertEquals(b, a);
        }

        // Now check that the header produced is of the length stated.
        ByteBuffer b = ByteBuffer
                        .wrap(new byte[] { headerBytes[8], headerBytes[9], headerBytes[10], headerBytes[11] });
        assertEquals(headerBytes.length, b.getInt());
    }

    @Test
    public void testTechnique2CRC() {
        // Expected result for this test.
        // originally expectedResult = { -47, -33, 95, -1, 0, 1, 0, 0, 0, 0, 0, 60, 0, 0, 0, 2, 1, 0, 0, 16, 0,
        byte[] expectedResult = new byte[] { -47, -33, 95, -1, 0, 1, 0, 0, 0, 0, 0, 60, 0, 0, 0, 2, 0, 4, 0, 16, 0,
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0,
                        -9, 105, 72, -88, -1, 95, -33, -47 };
        // expectedResult -22, -125, -49, -97, -1, 95, -33, -47 };

        // To create a header, we need to provide three techniques - the encapsulation, and the two
        // checksums (empty if not required).

        // Use technique 2 for this.
        Technique encapsulation = new Technique(Technique.TECHNIQUE_2, mask);

        // Pass values to header.
        Header header = new Header(encapsulation, "CRC32", "CRC32");

        // Get the constructed header in the form of a byte[] ready for output.
        byte[] headerBytes = header.getHeaderBytes();

        // Check the length.
        assertEquals("The length of the returned header was not as expected.", expectedResult.length,
                        headerBytes.length);

        // Check that each value is correct.
        for (int i = 0; i < headerBytes.length; i++) {
            int a = headerBytes[i];
            int b = expectedResult[i];
            assertEquals("One of the bytes within the processed header was not as expected.", b, a);
        }

        // Now check that the header produced is of the length stated.
        ByteBuffer b = ByteBuffer
                        .wrap(new byte[] { headerBytes[8], headerBytes[9], headerBytes[10], headerBytes[11] });
        assertEquals("The header length differed from the value within the header itself.", headerBytes.length,
                        b.getInt());
    }

    //
    // @Test
    // public void bitSetTest()
    // {
    // BitSet bitSet = new BitSet(16);
    //
    // // Set up for 128 bit mask.
    // bitSet.set(2);
    // // bitSet.set(2);
    // byte[] bytes = new byte[(bitSet.length() + 7) / 8];
    // for (int i = 0; i < bitSet.length(); i++)
    // {
    // if (bitSet.get(i))
    // {
    // bytes[bytes.length - i / 8 - 1] |= 1 << (i % 8);
    // }
    // }
    // FileOutputStream output = null;
    // try
    // {
    // output = new FileOutputStream("/releases/djrocke/argh/bitsetargh");
    // }
    // catch (FileNotFoundException e)
    // {
    // // TODO Auto-generated catch block
    // e.printStackTrace();
    // }
    // try
    // {
    // output.write(bytes);
    // }
    // catch (IOException e)
    // {
    // // TODO Auto-generated catch block
    // e.printStackTrace();
    // }
    //
    // }

    @Test
    public void testTechnique2SHA() {
        // Expected result for this test.
        // originally expectedResult = { -47, -33, 95, -1, 0, 1, 0, 0, 0, 0, 0, 88, 0, 0, 0, 2, 1, 0, 0, 16, 0,
        byte[] expectedResult = new byte[] { -47, -33, 95, -1, 0, 1, 0, 0, 0, 0, 0, 88, 0, 0, 0, 2, 0, 4, 0, 16, 0,
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0,
                        -23, 123, -30, -117, 75, -55, 37, -57, 78, 123, 103, -32, 25, -5, -29, 118, -10, -65, -102,
                        // 56, 81, 90, 79, 18, 126, -107, 60, -83, 100, 110, 120, 123, -11, 117, -113, -4, 35, -76,
                        -100, -4, -110, 78, 24, 110, 9, -76, 57, -117, 92, -12, -2, -1, 95, -33, -47 };
        // 113, 68, -98, 8, -90, -60, -80, 111, 100, 124, 57, 94, -108, -1, 95, -33, -47 };

        // To create a header, we need to provide three techniques - the encapsulation, and the two
        // checksums (empty if not required).

        // Use technique 2 for this.
        Technique encapsulation = new Technique(Technique.TECHNIQUE_2, mask);

        // Pass values to header.
        Header header = new Header(encapsulation, "SHA256", "SHA256");

        // Get the constructed header in the form of a byte[] ready for output.
        byte[] headerBytes = header.getHeaderBytes();

        // Check the length.
        assertEquals("The length of the returned header was not as expected.", expectedResult.length,
                        headerBytes.length);

        // Check that each value is correct.
        for (int i = 0; i < headerBytes.length; i++) {
            int a = headerBytes[i];
            int b = expectedResult[i];
            assertEquals("One of the bytes within the processed header was not as expected.", b, a);
        }

        // Now check that the header produced is of the length stated.
        ByteBuffer b = ByteBuffer
                        .wrap(new byte[] { headerBytes[8], headerBytes[9], headerBytes[10], headerBytes[11] });
        assertEquals("The header length differed from the value within the header itself.", headerBytes.length,
                        b.getInt());
    }
}
