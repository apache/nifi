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

package org.apache.nifi.wrapping.utils;
//DEV USE ONLY

//package org.apache.nifi.wrapping.utils;
//
//import static org.junit.Assert.assertTrue;
//import org.apache.nifi.wrapping.unwrapper.Unwrapper;
//import org.apache.nifi.wrapping.wrapper.Wrapper;
//import org.apache.nifi.wrapping.wrapper.WrapperFactory;
//import org.apache.nifi.wrapping.wrapper.WrapperFactory.CheckSum;
//import org.apache.nifi.wrapping.wrapper.WrapperFactory.MaskLength;
//import org.apache.nifi.wrapping.wrapper.WrapperFactory.TechniqueType;
//
//import java.io.ByteArrayInputStream;
//import java.io.ByteArrayOutputStream;
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.PipedInputStream;
//import java.io.PipedOutputStream;
//import java.util.Arrays;
//
//import org.apache.commons.lang.ArrayUtils;
//import org.junit.Before;
//import org.junit.Test;
//
//public class CompleteTest
//{
//  File inputFile = new File("src/test/resources/inputfile.gif");
//  byte[] inputBytes;
//
//  @Before
//  public void setUp() throws IOException
//  {
//    // Get the byte array representation of our test file.
//    InputStream is = new FileInputStream(inputFile);
//    long length = inputFile.length();
//    inputBytes = new byte[(int) length];
//    int offset = 0;
//    int numRead = 0;
//    while ((offset < inputBytes.length) && ((numRead = is.read(inputBytes, offset, inputBytes.length - offset)) >= 0))
//    {
//      offset += numRead;
//    }
//    is.close();
//  }
//
//  /**
//   * Check that we can successfully encrypt and decrypt a file using the default settings at the
//   * factory. Currently - tech 1 with no check sum.
//   *
//   * @throws IOException
//   */
//
//  public void testTechnique2IndexedMask() throws IOException
//  {
//    FileInputStream inputStream = new FileInputStream(inputFile);
//
//    ByteArrayOutputStream baos = new ByteArrayOutputStream();
//
//    File maskConfig = new File("src/test/resources/keys.cfg");
//
//    // Set up the wrapper.
//    WrapperFactory wrapperFactory = new WrapperFactory();
//
//    // Look for second key in this case.
//    Wrapper wrapper = wrapperFactory.getWrapper(baos, maskConfig, 2, CheckSum.CRC_32_CHECKSUM, CheckSum.CRC_32_CHECKSUM);
//
//    int read = 0;
//    while ((read = inputStream.read()) != -1)
//    {
//      wrapper.write(read);
//    }
//    wrapper.close();
//
//    byte[] encrypted = baos.toByteArray();
//
//    ByteArrayInputStream bais = new ByteArrayInputStream(encrypted);
//
//    // Set up the unwrapper.
//    Unwrapper unwrapper = new Unwrapper(bais, maskConfig);
//
//    int read2;
//    byte[] result = null;
//    while ((read2 = unwrapper.read()) != -1)
//    {
//
//      result = ArrayUtils.add(result, (byte) read2);
//    }
//    assertTrue(Arrays.equals(inputBytes, result));
//  }
//
//  @Test
//  public void testTechnique1SHAboth() throws IOException
//  {
//    FileInputStream inputStream = new FileInputStream(inputFile);
//
//    ByteArrayOutputStream baos = new ByteArrayOutputStream();
//
//    // Set up the wrapper.
//    WrapperFactory wrapperFactory = new WrapperFactory();
//    Wrapper wrapper = wrapperFactory.getWrapper(baos, MaskLength.MASK_128_BIT, TechniqueType.TECHNIQUE_1, CheckSum.SHA_256_CHECKSUM, CheckSum.SHA_256_CHECKSUM);
//
//    int read = 0;
//    while ((read = inputStream.read()) != -1)
//    {
//      wrapper.write(read);
//    }
//    wrapper.close();
//
//    byte[] encrypted = baos.toByteArray();
//
//    ByteArrayInputStream bais = new ByteArrayInputStream(encrypted);
//
//    // Set up the unwrapper.
//    Unwrapper unwrapper = new Unwrapper(bais);
//
//    int read2;
//    byte[] result = null;
//    while ((read2 = unwrapper.read()) != -1)
//    {
//
//      result = ArrayUtils.add(result, (byte) read2);
//    }
//    assertTrue(Arrays.equals(inputBytes, result));
//  }
//
//  /**
//   * Check that we can successfully encrypt and decrypt a file using the default settings at the
//   * factory. Currently - tech 1 with no check sum.
//   *
//   * @throws IOException
//   */
//  @Test
//  public void testStandardOperation() throws IOException
//  {
//    FileInputStream inputStream = new FileInputStream(inputFile);
//
//    ByteArrayOutputStream baos = new ByteArrayOutputStream();
//
//    // Set up the wrapper.
//    WrapperFactory wrapperFactory = new WrapperFactory();
//    Wrapper wrapper = wrapperFactory.getWrapper(baos);
//
//    int read = 0;
//    while ((read = inputStream.read()) != -1)
//    {
//      wrapper.write(read);
//    }
//    wrapper.close();
//
//    byte[] encrypted = baos.toByteArray();
//
//    ByteArrayInputStream bais = new ByteArrayInputStream(encrypted);
//
//    // Set up the unwrapper.
//    Unwrapper unwrapper = new Unwrapper(bais);
//
//    int read2;
//    byte[] result = null;
//    while ((read2 = unwrapper.read()) != -1)
//    {
//
//      result = ArrayUtils.add(result, (byte) read2);
//    }
//    assertTrue(Arrays.equals(inputBytes, result));
//  }
//
//  /**
//   * Check that we can successfully encrypt and decrypt a file using the default settings at the
//   * factory. Currently - tech 1 with no check sum.
//   *
//   * @throws IOException
//   */
//  @Test
//  public void testTechnique2Simple() throws IOException
//  {
//    FileInputStream inputStream = new FileInputStream(inputFile);
//
//    ByteArrayOutputStream baos = new ByteArrayOutputStream();
//
//    // Set up the wrapper.
//    WrapperFactory wrapperFactory = new WrapperFactory();
//    Wrapper wrapper = wrapperFactory.getWrapper(baos, TechniqueType.TECHNIQUE_2);
//
//    int read = 0;
//    while ((read = inputStream.read()) != -1)
//    {
//      wrapper.write(read);
//    }
//    wrapper.close();
//
//    byte[] encrypted = baos.toByteArray();
//
//    ByteArrayInputStream bais = new ByteArrayInputStream(encrypted);
//
//    // Set up the unwrapper.
//    Unwrapper unwrapper = new Unwrapper(bais);
//
//    int read2;
//    byte[] result = null;
//    while ((read2 = unwrapper.read()) != -1)
//    {
//
//      result = ArrayUtils.add(result, (byte) read2);
//    }
//    assertTrue(Arrays.equals(inputBytes, result));
//  }
//
//  @Test
//  public void testTechnique1default() throws IOException
//  {
//    FileInputStream inputStream = new FileInputStream(inputFile);
//
//    ByteArrayOutputStream baos = new ByteArrayOutputStream();
//
//    // Set up the wrapper.
//    WrapperFactory wrapperFactory = new WrapperFactory();
//    Wrapper wrapper = wrapperFactory.getWrapper(baos, TechniqueType.TECHNIQUE_1);
//
//    int read = 0;
//    while ((read = inputStream.read()) != -1)
//    {
//      wrapper.write(read);
//    }
//    wrapper.close();
//
//    byte[] encrypted = baos.toByteArray();
//
//    ByteArrayInputStream bais = new ByteArrayInputStream(encrypted);
//
//    // Set up the unwrapper.
//    Unwrapper unwrapper = new Unwrapper(bais);
//
//    int read2;
//    byte[] result = null;
//    while ((read2 = unwrapper.read()) != -1)
//    {
//
//      result = ArrayUtils.add(result, (byte) read2);
//    }
//    assertTrue(Arrays.equals(inputBytes, result));
//  }
//
//  @Test
//  public void testTechnique1CRCboth() throws IOException
//  {
//    FileInputStream inputStream = new FileInputStream(inputFile);
//
//    ByteArrayOutputStream baos = new ByteArrayOutputStream();
//
//    // Set up the wrapper.
//    WrapperFactory wrapperFactory = new WrapperFactory();
//    Wrapper wrapper = wrapperFactory.getWrapper(baos, MaskLength.MASK_128_BIT, TechniqueType.TECHNIQUE_1, CheckSum.CRC_32_CHECKSUM, CheckSum.CRC_32_CHECKSUM);
//
//    int read = 0;
//    while ((read = inputStream.read()) != -1)
//    {
//      wrapper.write(read);
//    }
//    wrapper.close();
//
//    byte[] encrypted = baos.toByteArray();
//
//    ByteArrayInputStream bais = new ByteArrayInputStream(encrypted);
//
//    // Set up the unwrapper.
//    Unwrapper unwrapper = new Unwrapper(bais);
//
//    int read2;
//    byte[] result = null;
//    while ((read2 = unwrapper.read()) != -1)
//    {
//
//      result = ArrayUtils.add(result, (byte) read2);
//    }
//    assertTrue(Arrays.equals(inputBytes, result));
//  }
//
//  @Test
//  public void testTechnique1CRChead() throws IOException
//  {
//    FileInputStream inputStream = new FileInputStream(inputFile);
//
//    ByteArrayOutputStream baos = new ByteArrayOutputStream();
//
//    // Set up the wrapper.
//    WrapperFactory wrapperFactory = new WrapperFactory();
//    Wrapper wrapper = wrapperFactory.getWrapper(baos, MaskLength.MASK_128_BIT, TechniqueType.TECHNIQUE_1, CheckSum.CRC_32_CHECKSUM, CheckSum.NO_CHECKSUM);
//
//    int read = 0;
//    while ((read = inputStream.read()) != -1)
//    {
//      wrapper.write(read);
//    }
//    wrapper.close();
//
//    byte[] encrypted = baos.toByteArray();
//
//    ByteArrayInputStream bais = new ByteArrayInputStream(encrypted);
//
//    // Set up the unwrapper.
//    Unwrapper unwrapper = new Unwrapper(bais);
//
//    int read2;
//    byte[] result = null;
//    while ((read2 = unwrapper.read()) != -1)
//    {
//
//      result = ArrayUtils.add(result, (byte) read2);
//    }
//    assertTrue(Arrays.equals(inputBytes, result));
//  }
//
//  @Test
//  public void testTechnique1CRCbody() throws IOException
//  {
//    FileInputStream inputStream = new FileInputStream(inputFile);
//
//    ByteArrayOutputStream baos = new ByteArrayOutputStream();
//
//    // Set up the wrapper.
//    WrapperFactory wrapperFactory = new WrapperFactory();
//    Wrapper wrapper = wrapperFactory.getWrapper(baos, MaskLength.MASK_128_BIT, TechniqueType.TECHNIQUE_1, CheckSum.NO_CHECKSUM, CheckSum.CRC_32_CHECKSUM);
//
//    int read = 0;
//    while ((read = inputStream.read()) != -1)
//    {
//      wrapper.write(read);
//    }
//    wrapper.close();
//
//    byte[] encrypted = baos.toByteArray();
//
//    ByteArrayInputStream bais = new ByteArrayInputStream(encrypted);
//
//    // Set up the unwrapper.
//    Unwrapper unwrapper = new Unwrapper(bais);
//
//    int read2;
//    byte[] result = null;
//    while ((read2 = unwrapper.read()) != -1)
//    {
//
//      result = ArrayUtils.add(result, (byte) read2);
//    }
//    assertTrue(Arrays.equals(inputBytes, result));
//  }
//
//  @Test
//  public void testTechnique1CRCbodySHAhead() throws IOException
//  {
//    FileInputStream inputStream = new FileInputStream(inputFile);
//
//    ByteArrayOutputStream baos = new ByteArrayOutputStream();
//
//    // Set up the wrapper.
//    WrapperFactory wrapperFactory = new WrapperFactory();
//    Wrapper wrapper = wrapperFactory.getWrapper(baos, MaskLength.MASK_128_BIT, TechniqueType.TECHNIQUE_1, CheckSum.SHA_256_CHECKSUM, CheckSum.CRC_32_CHECKSUM);
//
//    int read = 0;
//    while ((read = inputStream.read()) != -1)
//    {
//      wrapper.write(read);
//    }
//    wrapper.close();
//
//    byte[] encrypted = baos.toByteArray();
//
//    ByteArrayInputStream bais = new ByteArrayInputStream(encrypted);
//
//    // Set up the unwrapper.
//    Unwrapper unwrapper = new Unwrapper(bais);
//
//    int read2;
//    byte[] result = null;
//    while ((read2 = unwrapper.read()) != -1)
//    {
//
//      result = ArrayUtils.add(result, (byte) read2);
//    }
//    assertTrue(Arrays.equals(inputBytes, result));
//  }
//
//  @Test
//  public void testTechnique1SHAbodyCRChead() throws IOException
//  {
//    FileInputStream inputStream = new FileInputStream(inputFile);
//
//    ByteArrayOutputStream baos = new ByteArrayOutputStream();
//
//    // Set up the wrapper.
//    WrapperFactory wrapperFactory = new WrapperFactory();
//    Wrapper wrapper = wrapperFactory.getWrapper(baos, MaskLength.MASK_256_BIT, TechniqueType.TECHNIQUE_1, CheckSum.CRC_32_CHECKSUM, CheckSum.SHA_256_CHECKSUM);
//
//    int read = 0;
//    while ((read = inputStream.read()) != -1)
//    {
//      wrapper.write(read);
//    }
//    wrapper.close();
//
//    byte[] encrypted = baos.toByteArray();
//
//    ByteArrayInputStream bais = new ByteArrayInputStream(encrypted);
//
//    // Set up the unwrapper.
//    Unwrapper unwrapper = new Unwrapper(bais);
//
//    int read2;
//    byte[] result = null;
//    while ((read2 = unwrapper.read()) != -1)
//    {
//
//      result = ArrayUtils.add(result, (byte) read2);
//    }
//    assertTrue(Arrays.equals(inputBytes, result));
//  }
//
//  @Test
//  public void testTechnique2default() throws IOException
//  {
//    FileInputStream inputStream = new FileInputStream(inputFile);
//
//    ByteArrayOutputStream baos = new ByteArrayOutputStream();
//
//    // Set up the wrapper.
//    WrapperFactory wrapperFactory = new WrapperFactory();
//    Wrapper wrapper = wrapperFactory.getWrapper(baos, TechniqueType.TECHNIQUE_2);
//
//    int read = 0;
//    while ((read = inputStream.read()) != -1)
//    {
//      wrapper.write(read);
//    }
//    wrapper.close();
//
//    byte[] encrypted = baos.toByteArray();
//
//    ByteArrayInputStream bais = new ByteArrayInputStream(encrypted);
//
//    // Set up the unwrapper.
//    Unwrapper unwrapper = new Unwrapper(bais);
//
//    int read2;
//    byte[] result = null;
//    while ((read2 = unwrapper.read()) != -1)
//    {
//
//      result = ArrayUtils.add(result, (byte) read2);
//    }
//    assertTrue(Arrays.equals(inputBytes, result));
//  }
//
//  @Test
//  public void testTechnique2CRCboth() throws IOException
//  {
//    FileInputStream inputStream = new FileInputStream(inputFile);
//
//    ByteArrayOutputStream baos = new ByteArrayOutputStream();
//
//    // Set up the wrapper.
//    WrapperFactory wrapperFactory = new WrapperFactory();
//    Wrapper wrapper = wrapperFactory.getWrapper(baos, MaskLength.MASK_128_BIT, TechniqueType.TECHNIQUE_2, CheckSum.CRC_32_CHECKSUM, CheckSum.SHA_256_CHECKSUM);
//
//    int read = 0;
//    while ((read = inputStream.read()) != -1)
//    {
//      wrapper.write(read);
//    }
//    wrapper.close();
//
//    byte[] encrypted = baos.toByteArray();
//
//    ByteArrayInputStream bais = new ByteArrayInputStream(encrypted);
//
//    // Set up the unwrapper.
//    Unwrapper unwrapper = new Unwrapper(bais);
//
//    int read2;
//    byte[] result = null;
//    while ((read2 = unwrapper.read()) != -1)
//    {
//
//      result = ArrayUtils.add(result, (byte) read2);
//    }
//    assertTrue(Arrays.equals(inputBytes, result));
//  }
//
//  @Test
//  public void testTechnique2CRChead() throws IOException
//  {
//    FileInputStream inputStream = new FileInputStream(inputFile);
//
//    ByteArrayOutputStream baos = new ByteArrayOutputStream();
//
//    // Set up the wrapper.
//    WrapperFactory wrapperFactory = new WrapperFactory();
//    Wrapper wrapper = wrapperFactory.getWrapper(baos, MaskLength.MASK_128_BIT, TechniqueType.TECHNIQUE_2, CheckSum.CRC_32_CHECKSUM, CheckSum.NO_CHECKSUM);
//
//    int read = 0;
//    while ((read = inputStream.read()) != -1)
//    {
//      wrapper.write(read);
//    }
//    wrapper.close();
//
//    byte[] encrypted = baos.toByteArray();
//
//    ByteArrayInputStream bais = new ByteArrayInputStream(encrypted);
//
//    // Set up the unwrapper.
//    Unwrapper unwrapper = new Unwrapper(bais);
//
//    int read2;
//    byte[] result = null;
//    while ((read2 = unwrapper.read()) != -1)
//    {
//
//      result = ArrayUtils.add(result, (byte) read2);
//    }
//    assertTrue(Arrays.equals(inputBytes, result));
//  }
//
//  @Test
//  public void testTechnique2CRCbody() throws IOException
//  {
//    FileInputStream inputStream = new FileInputStream(inputFile);
//
//    ByteArrayOutputStream baos = new ByteArrayOutputStream();
//
//    // Set up the wrapper.
//    WrapperFactory wrapperFactory = new WrapperFactory();
//    Wrapper wrapper = wrapperFactory.getWrapper(baos, MaskLength.MASK_128_BIT, TechniqueType.TECHNIQUE_2, CheckSum.NO_CHECKSUM, CheckSum.CRC_32_CHECKSUM);
//
//    int read = 0;
//    while ((read = inputStream.read()) != -1)
//    {
//      wrapper.write(read);
//    }
//    wrapper.close();
//
//    byte[] encrypted = baos.toByteArray();
//
//    ByteArrayInputStream bais = new ByteArrayInputStream(encrypted);
//
//    // Set up the unwrapper.
//    Unwrapper unwrapper = new Unwrapper(bais);
//
//    int read2;
//    byte[] result = null;
//    while ((read2 = unwrapper.read()) != -1)
//    {
//
//      result = ArrayUtils.add(result, (byte) read2);
//    }
//    assertTrue(Arrays.equals(inputBytes, result));
//  }
//
//  @Test
//  public void testTechnique2CRCbodySHAhead() throws IOException
//  {
//    FileInputStream inputStream = new FileInputStream(inputFile);
//
//    ByteArrayOutputStream baos = new ByteArrayOutputStream();
//
//    // Set up the wrapper.
//    WrapperFactory wrapperFactory = new WrapperFactory();
//    Wrapper wrapper = wrapperFactory.getWrapper(baos, MaskLength.MASK_128_BIT, TechniqueType.TECHNIQUE_2, CheckSum.SHA_256_CHECKSUM, CheckSum.CRC_32_CHECKSUM);
//
//    int read = 0;
//    while ((read = inputStream.read()) != -1)
//    {
//      wrapper.write(read);
//    }
//    wrapper.close();
//
//    byte[] encrypted = baos.toByteArray();
//
//    ByteArrayInputStream bais = new ByteArrayInputStream(encrypted);
//
//    // Set up the unwrapper.
//    Unwrapper unwrapper = new Unwrapper(bais);
//
//    int read2;
//    byte[] result = null;
//    while ((read2 = unwrapper.read()) != -1)
//    {
//
//      result = ArrayUtils.add(result, (byte) read2);
//    }
//    assertTrue(Arrays.equals(inputBytes, result));
//  }
//
//  @Test
//  public void testTechnique2SHAbodyCRChead() throws IOException
//  {
//    FileInputStream inputStream = new FileInputStream(inputFile);
//
//    ByteArrayOutputStream baos = new ByteArrayOutputStream();
//
//    // Set up the wrapper.
//    WrapperFactory wrapperFactory = new WrapperFactory();
//    Wrapper wrapper = wrapperFactory.getWrapper(baos, MaskLength.MASK_128_BIT, TechniqueType.TECHNIQUE_2, CheckSum.CRC_32_CHECKSUM, CheckSum.SHA_256_CHECKSUM);
//
//    int read = 0;
//    while ((read = inputStream.read()) != -1)
//    {
//      wrapper.write(read);
//    }
//    wrapper.close();
//
//    byte[] encrypted = baos.toByteArray();
//
//    ByteArrayInputStream bais = new ByteArrayInputStream(encrypted);
//
//    // Set up the unwrapper.
//    Unwrapper unwrapper = new Unwrapper(bais);
//
//    int read2;
//    byte[] result = null;
//    while ((read2 = unwrapper.read()) != -1)
//    {
//
//      result = ArrayUtils.add(result, (byte) read2);
//    }
//    assertTrue(Arrays.equals(inputBytes, result));
//  }
//
//  @Test
//  public void testTechnique2SHAboth() throws IOException
//  {
//    FileInputStream inputStream = new FileInputStream(inputFile);
//
//    ByteArrayOutputStream baos = new ByteArrayOutputStream();
//
//    // Set up the wrapper.
//    WrapperFactory wrapperFactory = new WrapperFactory();
//    Wrapper wrapper = wrapperFactory.getWrapper(baos, MaskLength.MASK_128_BIT, TechniqueType.TECHNIQUE_2, CheckSum.SHA_256_CHECKSUM, CheckSum.SHA_256_CHECKSUM);
//
//    int read = 0;
//    while ((read = inputStream.read()) != -1)
//    {
//      wrapper.write(read);
//    }
//    wrapper.close();
//
//    byte[] encrypted = baos.toByteArray();
//
//    ByteArrayInputStream bais = new ByteArrayInputStream(encrypted);
//
//    // Set up the unwrapper.
//    Unwrapper unwrapper = new Unwrapper(bais);
//
//    int read2;
//    byte[] result = null;
//    while ((read2 = unwrapper.read()) != -1)
//    {
//
//      result = ArrayUtils.add(result, (byte) read2);
//    }
//    assertTrue(Arrays.equals(inputBytes, result));
//  }
//
//  @Test
//  public void speedTest() throws IOException
//  {
//    final int megabytesToWrite = 100;
//
//    final int bytesToWrite = megabytesToWrite * 1048576;
//
//    PipedInputStream pipeIn = new PipedInputStream();
//    PipedOutputStream pipeOut = new PipedOutputStream(pipeIn);
//    // Set up the wrapper.
//    WrapperFactory wrapperFactory = new WrapperFactory();
//    final Wrapper wrapper = wrapperFactory.getWrapper(pipeOut);
//    long startTime = System.currentTimeMillis();
//
//    new Thread(new Runnable()
//    {
//      public void run()
//      {
//        try
//        {
//          for (int i = 0; i < bytesToWrite; i++)
//          {
//            double value = (5 + Math.random()) * 15;
//
//            wrapper.write((int) value);
//          }
//
//          wrapper.flush();
//          wrapper.close();
//        }
//        catch (IOException e)
//        {
//          // TODO Auto-generated catch block
//          e.printStackTrace();
//        }
//      }
//    }).start();
//
//    Unwrapper unwrapper = new Unwrapper(pipeIn);
//
//    // Get an output stream to use
//
//    try
//    {
//      while ((unwrapper.read()) != -1)
//      {
//        // can print if we need.
//
//      }
//    }
//    catch (IOException e)
//    {
//
//    }
//    wrapper.close();
//    long endTime = System.currentTimeMillis();
//
//    long elapsedTime = endTime - startTime;
//
//    System.out.println("Processing time with for " + megabytesToWrite + " megabytes was: " + (elapsedTime / 1000) + " seconds");
//  }
// }
