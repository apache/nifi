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
package org.apache.nifi.processors.standard.util;

import org.apache.nifi.processors.standard.SplitLargeJson;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.json.stream.JsonParser;

public class TestJsonStack {

    @Test
    public void stackPrint() {
        JsonStack stack = new JsonStack();

        SplitLargeJson.JsonParserView view = Mockito.mock(SplitLargeJson.JsonParserView.class);
        stack.receive(JsonParser.Event.START_ARRAY, view);
        stack.receive(JsonParser.Event.START_OBJECT, view);

        Mockito.when(view.getString()).thenReturn("key1");
        stack.receive(JsonParser.Event.KEY_NAME, view);

        Assert.assertEquals("$[0].key1", stack.toString());
    }

    @Test
    public void startsWith() {
        JsonStack stack = new JsonStack();

        SplitLargeJson.JsonParserView view = Mockito.mock(SplitLargeJson.JsonParserView.class);
        stack.receive(JsonParser.Event.START_ARRAY, view);
        stack.receive(JsonParser.Event.START_OBJECT, view);

        Mockito.when(view.getString()).thenReturn("key1");
        stack.receive(JsonParser.Event.KEY_NAME, view);
        stack.receive(JsonParser.Event.VALUE_TRUE, view);
        stack.receive(JsonParser.Event.END_OBJECT, view);
        stack.receive(JsonParser.Event.START_OBJECT, view);

        Assert.assertTrue(stack.startsWith(SimpleJsonPath.of("$[1]")));
    }
}
