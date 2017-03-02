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

import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.SharedSessionState;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class OldestFirstPrioritizerTest {

    @Test
    public void testPrioritizer() throws InstantiationException, IllegalAccessException {
        final Processor processor = new SimpleProcessor();
        final AtomicLong idGenerator = new AtomicLong(0L);
        final MockProcessSession session = new MockProcessSession(new SharedSessionState(processor, idGenerator), Mockito.mock(Processor.class));

        final MockFlowFile flowFile1 = session.create();
        try {
            Thread.sleep(2); // guarantee the FlowFile entryDate for flowFile2 is different than flowFile1
        } catch (final InterruptedException e) {
        }
        final MockFlowFile flowFile2 = session.create();

        final OldestFlowFileFirstPrioritizer prioritizer = new OldestFlowFileFirstPrioritizer();
        Assert.assertEquals(0, prioritizer.compare(null, null));
        Assert.assertEquals(-1, prioritizer.compare(flowFile1, null));
        Assert.assertEquals(1, prioritizer.compare(null, flowFile1));
        Assert.assertEquals(0, prioritizer.compare(flowFile1, flowFile1));
        Assert.assertEquals(0, prioritizer.compare(flowFile2, flowFile2));
        Assert.assertEquals(-1, prioritizer.compare(flowFile1, flowFile2));
        Assert.assertEquals(1, prioritizer.compare(flowFile2, flowFile1));
    }

    public class SimpleProcessor extends AbstractProcessor {

        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        }

    }

}
