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
package org.apache.nifi.prioritizer;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.SharedSessionState;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class PriorityAttributePrioritizerTest {

    static Map<String, String> attrsPri1 = new HashMap<String, String>();
    static Map<String, String> attrsPri2 = new HashMap<String, String>();
    static Map<String, String> attrsPrin1 = new HashMap<String, String>();
    static Map<String, String> attrsPriA = new HashMap<String, String>();
    static Map<String, String> attrsPriB = new HashMap<String, String>();
    static Map<String, String> attrsPriLP = new HashMap<String, String>();
    static Map<String, String> attrsPriLN = new HashMap<String, String>();

    @BeforeClass
    public static void init() {
        attrsPri1.put(CoreAttributes.PRIORITY.key(), "1");
        attrsPri2.put(CoreAttributes.PRIORITY.key(), "2");
        attrsPrin1.put(CoreAttributes.PRIORITY.key(), "-1");
        attrsPriA.put(CoreAttributes.PRIORITY.key(), "A");
        attrsPriB.put(CoreAttributes.PRIORITY.key(), "B");
        attrsPriLP.put(CoreAttributes.PRIORITY.key(), "5432123456789");
        attrsPriLN.put(CoreAttributes.PRIORITY.key(), "-5432123456789");
    }

    @Test
    public void testPrioritizer() throws InstantiationException, IllegalAccessException {
        final Processor processor = new SimpleProcessor();
        final AtomicLong idGenerator = new AtomicLong(0L);
        final MockProcessSession session = new MockProcessSession(new SharedSessionState(processor, idGenerator), Mockito.mock(Processor.class));

        final MockFlowFile ffNoPriority = session.create();
        final MockFlowFile ffPri1 = session.create();
        ffPri1.putAttributes(attrsPri1);
        final MockFlowFile ffPri2 = session.create();
        ffPri2.putAttributes(attrsPri2);
        final MockFlowFile ffPrin1 = session.create();
        ffPrin1.putAttributes(attrsPrin1);
        final MockFlowFile ffPriA = session.create();
        ffPriA.putAttributes(attrsPriA);
        final MockFlowFile ffPriB = session.create();
        ffPriB.putAttributes(attrsPriB);
        final MockFlowFile ffPriLP = session.create();
        ffPriLP.putAttributes(attrsPriLP);
        final MockFlowFile ffPriLN = session.create();
        ffPriLN.putAttributes(attrsPriLN);

        final PriorityAttributePrioritizer prioritizer = new PriorityAttributePrioritizer();
        assertEquals(0, prioritizer.compare(null, null));
        assertEquals(-1, prioritizer.compare(ffNoPriority, null));
        assertEquals(1, prioritizer.compare(null, ffNoPriority));

        assertEquals(-1, prioritizer.compare(ffNoPriority, ffNoPriority));
        assertEquals(-1, prioritizer.compare(ffPri1, ffNoPriority));
        assertEquals(1, prioritizer.compare(ffNoPriority, ffPri1));

        assertEquals(0, prioritizer.compare(ffPri1, ffPri1));
        assertEquals(-1, prioritizer.compare(ffPri1, ffPri2));
        assertEquals(1, prioritizer.compare(ffPri2, ffPri1));
        assertEquals(-1, prioritizer.compare(ffPrin1, ffPri1));
        assertEquals(1, prioritizer.compare(ffPri1, ffPrin1));

        assertEquals(-1, prioritizer.compare(ffPri1, ffPriA));
        assertEquals(1, prioritizer.compare(ffPriA, ffPri1));

        assertEquals(0, prioritizer.compare(ffPriA, ffPriA));
        assertEquals(-1, prioritizer.compare(ffPriA, ffPriB));
        assertEquals(1, prioritizer.compare(ffPriB, ffPriA));

        assertEquals(1, prioritizer.compare(ffPriLP, ffPri1));
        assertEquals(-1, prioritizer.compare(ffPri1, ffPriLP));
        assertEquals(-1, prioritizer.compare(ffPriLN, ffPri1));
        assertEquals(1, prioritizer.compare(ffPri1, ffPriLN));
    }

    public class SimpleProcessor extends AbstractProcessor {

        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        }

    }

}
