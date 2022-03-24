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
package org.apache.nifi.toolkit.cli.impl.command;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestCommandProcessor {

    @Test
    public void testCommandProcessor() throws ParseException {
        final List<String> results = new ArrayList<>();
        results.add("foo1");
        results.add("foo2");

        final CommandA command = new CommandA(results);

        final Context context = Mockito.mock(Context.class);
        Mockito.when(context.getOutput()).thenReturn(System.out);

        // run the command once to set the previous results
        final CommandProcessor processor = new CommandProcessor(Collections.emptyMap(), Collections.emptyMap(), context);
        processor.processCommand(new String[] {}, command);

        // run it again and &1 should be resolved to foo1
        processor.processCommand(
                new String[] {
                        "-" + CommandOption.BUCKET_ID.getShortName(),
                        "&1"
                } ,
                command);

        final CommandLine cli1 = command.getCli();
        Assert.assertEquals("foo1", cli1.getOptionValue(CommandOption.BUCKET_ID.getShortName()));

        // run it again and &2 should be resolved to foo1
        processor.processCommand(
                new String[] {
                        "-" + CommandOption.BUCKET_ID.getShortName(),
                        "&2"
                },
                command);

        final CommandLine cli2 = command.getCli();
        Assert.assertEquals("foo2", cli2.getOptionValue(CommandOption.BUCKET_ID.getShortName()));

        // run it again and &1 should be resolved to foo1
        processor.processCommand(
                new String[] {
                        "-" + CommandOption.BUCKET_ID.getShortName(),
                        "b1",
                        "-" + CommandOption.FLOW_ID.getShortName(),
                        "&1"
                },
                command);

        final CommandLine cli3 = command.getCli();
        Assert.assertEquals("foo1", cli3.getOptionValue(CommandOption.FLOW_ID.getShortName()));
    }

}
