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

package org.apache.nifi.py4j.client;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestCommandBuilder {

    @Test
    public void testObjectsBound() {
        final JavaObjectBindings bindings = new JavaObjectBindings();
        final String objectId = "oTestObject1";
        final String methodName = "testMethod";

        final CommandBuilder builder = new CommandBuilder(bindings, objectId, methodName);
        final Iterable<Object> iterable = new Iterable<>() {
            @Override
            public Iterator<Object> iterator() {
                return null;
            }
        };

        final Object[] args = new Object[] {"string", 1, 1.0F, 1.0D, 1L, null, true, 'c', (byte) 1, (short) 1, "hello world".getBytes(),
            new Object(), new int[2], new Object[2], new ArrayList<>(), new HashSet<>(), iterable};

        final String command = builder.buildCommand(args);

        // Ensure that we bind an ID for each object (except for String and byte[]) and not the primitives.
        // So we should have a bound ID for each of the following:
        // new Object()
        // new int[2]
        // new Object[2]
        // new ArrayList<>()
        // new HashSet<>()
        // iterable
        assertEquals(6, builder.getBoundIds().size());

        // Ensure that the command has the appropriate header
        final String[] commandSplits = command.split("\n");
        assertEquals(CommandBuilder.METHOD_INVOCATION_START_CLAUSE.trim(), commandSplits[0]);
        assertEquals(objectId, commandSplits[1]);
        assertEquals(methodName, commandSplits[2]);

        // Verify the primitive arguments
        assertTrue(commandSplits[3].contains("string"));
        assertTrue(commandSplits[4].contains("1"));
        assertTrue(commandSplits[5].contains("1.0"));
        assertTrue(commandSplits[6].contains("1.0"));
        assertTrue(commandSplits[7].contains("1"));
        assertEquals("n", commandSplits[8]); // n is used for null values
        assertTrue(commandSplits[9].contains("true"));
        assertTrue(commandSplits[10].contains("c"));
        assertTrue(commandSplits[11].contains("1"));
        assertTrue(commandSplits[12].contains("1"));

        final List<String> boundIds = builder.getBoundIds();

        // Verify the object arguments
        for (int splitIndex = 14; splitIndex < commandSplits.length - 1; splitIndex++) {
            final String argId = commandSplits[splitIndex].substring(1);
            assertTrue(boundIds.contains(argId));
            assertSame(args[splitIndex - 3], bindings.getBoundObject(argId));
        }

        // Ensure that the command ends with the appropriate footer
        assertEquals(CommandBuilder.METHOD_INVOCATION_END_CLAUSE.trim(), commandSplits[commandSplits.length - 1]);
    }
}
