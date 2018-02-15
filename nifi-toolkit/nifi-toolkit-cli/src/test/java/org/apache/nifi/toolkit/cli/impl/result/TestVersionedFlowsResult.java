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

import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.api.ReferenceResolver;
import org.apache.nifi.toolkit.cli.api.ResultType;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TestVersionedFlowsResult {

    private ByteArrayOutputStream outputStream;
    private PrintStream printStream;
    private List<VersionedFlow> flows;

    @Before
    public void setup() {
        this.outputStream = new ByteArrayOutputStream();
        this.printStream = new PrintStream(outputStream, true);

        final VersionedFlow f1 = new VersionedFlow();
        f1.setName("Flow 1");
        f1.setDescription("This is flow 1");
        f1.setIdentifier(UUID.fromString("ea752054-22c6-4fc0-b851-967d9a3837cb").toString());
        f1.setBucketIdentifier("b1");
        f1.setBucketName("Bucket 1");

        final VersionedFlow f2 = new VersionedFlow();
        f2.setName("Flow 2");
        f2.setDescription(null);
        f2.setIdentifier(UUID.fromString("ddf5f289-7502-46df-9798-4b0457c1816b").toString());
        f2.setBucketIdentifier("b2");
        f2.setBucketName("Bucket 2");

        this.flows = new ArrayList<>();
        flows.add(f1);
        flows.add(f2);
    }

    @Test
    public void testWriteSimpleVersionedFlowsResult() throws IOException {
        final VersionedFlowsResult result = new VersionedFlowsResult(ResultType.SIMPLE, flows);
        result.write(printStream);

        final String resultOut = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
        //System.out.println(resultOut);

        final String expected = "\n" +
                "#   Name     Id                                     Description      \n" +
                "-   ------   ------------------------------------   --------------   \n" +
                "1   Flow 1   ea752054-22c6-4fc0-b851-967d9a3837cb   This is flow 1   \n" +
                "2   Flow 2   ddf5f289-7502-46df-9798-4b0457c1816b   (empty)          \n" +
                "\n";

        Assert.assertEquals(expected, resultOut);
    }

    @Test
    public void testReferenceResolver() {
        final VersionedFlowsResult result = new VersionedFlowsResult(ResultType.SIMPLE, flows);
        final ReferenceResolver resolver = result.createReferenceResolver(Mockito.mock(Context.class));

        // should default to flow id when no option is specified
        Assert.assertEquals("ea752054-22c6-4fc0-b851-967d9a3837cb", resolver.resolve(null, 1).getResolvedValue());
        Assert.assertEquals("ddf5f289-7502-46df-9798-4b0457c1816b", resolver.resolve(null, 2).getResolvedValue());

        // should use flow id when flow id is specified
        Assert.assertEquals("ea752054-22c6-4fc0-b851-967d9a3837cb", resolver.resolve(CommandOption.FLOW_ID, 1).getResolvedValue());
        Assert.assertEquals("ddf5f289-7502-46df-9798-4b0457c1816b", resolver.resolve(CommandOption.FLOW_ID, 2).getResolvedValue());

        // should resolve the bucket id when bucket id option is used
        Assert.assertEquals("b1", resolver.resolve(CommandOption.BUCKET_ID, 1).getResolvedValue());
        Assert.assertEquals("b2", resolver.resolve(CommandOption.BUCKET_ID, 2).getResolvedValue());

        // should resolve to null when position doesn't exist
        Assert.assertEquals(null, resolver.resolve(CommandOption.FLOW_ID, 3));
    }

}
