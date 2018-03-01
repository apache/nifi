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
package org.apache.nifi.toolkit.cli.impl.result;

import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.toolkit.cli.api.ResultType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TestBucketsResult {

    private ByteArrayOutputStream outputStream;
    private PrintStream printStream;

    @Before
    public void setup() {
        this.outputStream = new ByteArrayOutputStream();
        this.printStream = new PrintStream(outputStream, true);
    }

    @Test
    public void testWritingSimpleBucketsResult() throws IOException {
        final Bucket b1 = new Bucket();
        b1.setName("Bucket 1");
        b1.setDescription("This is bucket 1");
        b1.setIdentifier(UUID.fromString("ea752054-22c6-4fc0-b851-967d9a3837cb").toString());

        final Bucket b2 = new Bucket();
        b2.setName("Bucket 2");
        b2.setDescription(null);
        b2.setIdentifier(UUID.fromString("ddf5f289-7502-46df-9798-4b0457c1816b").toString());

        final List<Bucket> buckets = new ArrayList<>();
        buckets.add(b1);
        buckets.add(b2);

        final BucketsResult result = new BucketsResult(ResultType.SIMPLE, buckets);
        result.write(printStream);

        final String resultOut = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
        //System.out.println(resultOut);

        final String expected = "\n" +
                "#   Name       Id                                     Description        \n" +
                "-   --------   ------------------------------------   ----------------   \n" +
                "1   Bucket 1   ea752054-22c6-4fc0-b851-967d9a3837cb   This is bucket 1   \n" +
                "2   Bucket 2   ddf5f289-7502-46df-9798-4b0457c1816b   (empty)            \n" +
                "\n";

        Assert.assertEquals(expected, resultOut);
    }

}
