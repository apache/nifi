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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.wrapping.unwrapper.Unwrapper;
import org.apache.nifi.wrapping.wrapper.Wrapper;
import org.apache.nifi.wrapping.wrapper.WrapperFactory;
import org.apache.nifi.wrapping.wrapper.WrapperFactory.CheckSum;
import org.apache.nifi.wrapping.wrapper.WrapperFactory.MaskLength;
import org.apache.nifi.wrapping.wrapper.WrapperFactory.TechniqueType;
import org.junit.Test;

/**
 * Runs a variety of performance tests against the wrapping algorithm.
 */
public class PerformanceTester {

  final float megabytesToWrite = 50;

  @Test
  public void technique2WrapTest() throws IOException {

    BufferedInputStream bufferedIn = new BufferedInputStream(new GarbageInputStream());

    // Output to null.
    BufferedOutputStream bufferedOut = new BufferedOutputStream(new NullOutputStream());

    // BufferedInputStream bufferedIn = new BufferedInputStream(new FileInputStream(inputFile));
    // BufferedOutputStream bufferedOut = new BufferedOutputStream(new
    // FileOutputStream(wrappedFile));

    byte[] buffer = new byte[8192];

    long startTime = System.currentTimeMillis();

    // Set up the wrapper.
    WrapperFactory wrapperFactory = new WrapperFactory();
    Wrapper wrapper = wrapperFactory.getWrapper(bufferedOut, MaskLength.MASK_128_BIT, TechniqueType.TECHNIQUE_2, CheckSum.CRC_32_CHECKSUM, CheckSum.CRC_32_CHECKSUM);

    float megaBytesChewed = 0;
    float test = new Float(1048576f);

    for (int got; (got = bufferedIn.read(buffer)) > 0;) {
      while (megaBytesChewed < megabytesToWrite) {
        wrapper.write(buffer);

        megaBytesChewed += got / test;
      }
      break;
    }

    long endTime = System.currentTimeMillis();
    long elapsedTime = endTime - startTime;
    wrapper.flush();
    wrapper.close();

    System.out.println("Time taken to wrap " + megabytesToWrite + "mb of data was: " + (elapsedTime / 1000) + " seconds");
  }

  @Test
  public void technique2UnwrapTest() throws IOException {

    byte[] headerCRC = new byte[] { -47, -33, 95, -1, 0, 1, 0, 0, 0, 0, 0, 64, 0, 0, 0, 2, 1, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0,
        0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, -114, -31, -77, -100, -1, 95, -33, -47 };

    // Get the input stream from the byte array.
    // Buffer it up as needed.
    InputStream headerIn = new BufferedInputStream(new ByteArrayInputStream(headerCRC));

    InputStream garbageIn = new BufferedInputStream(new GarbageInputStream());

    BufferedOutputStream bufferedOut = new BufferedOutputStream(new NullOutputStream());

    // BufferedInputStream bufferedIn = new BufferedInputStream(new
    // FileInputStream("/home/djrocke/nifi/wrapped"));
    // FileOutputStream fileOut = new FileOutputStream("/home/djrocke/nifi/unwrapped");

    long startTime = System.currentTimeMillis();

    // Use to speed up load through algorithm. File input is buffered - we need to do same going
    // into unwrapper.
    byte[] buffer = new byte[8192];

    List<InputStream> streams = Arrays.asList(headerIn, garbageIn);
    SequenceInputStream streamseq = new SequenceInputStream(Collections.enumeration(streams));

    // Set up the unwrapper.
    // Unwrapper unwrapper = new Unwrapper(bufferedIn, fileOut);
    Unwrapper unwrapper = new Unwrapper(streamseq, bufferedOut);

    // long bytesChewed = 0;

    // while ((unwrapper.read(buffer)) != -1)
    // {
    // We don't need to do anything here - unwrapper handles output.
    // }
    double megaBytesChewed = 0;
    float test = new Float(1048576f);
    while (megaBytesChewed < megabytesToWrite) {
      megaBytesChewed += ((unwrapper.read(buffer)) / test);
    }

    long endTime = System.currentTimeMillis();
    long elapsedTime = endTime - startTime;

    System.out.println("Time taken to unwrap " + megabytesToWrite + "mb of data was: " + (elapsedTime / 1000) + " seconds");
  }
}