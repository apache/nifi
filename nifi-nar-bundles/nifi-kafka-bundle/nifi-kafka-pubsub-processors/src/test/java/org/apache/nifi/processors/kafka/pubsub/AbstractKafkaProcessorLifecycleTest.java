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
package org.apache.nifi.processors.kafka.pubsub;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.Closeable;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class AbstractKafkaProcessorLifecycleTest {

    private final static Random random = new Random();

    @Test
    public void validateBaseProperties() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(DummyProcessor.class);
        runner.setProperty(AbstractKafkaProcessor.BOOTSTRAP_SERVERS, "");
        runner.setProperty(AbstractKafkaProcessor.TOPIC, "foo");
        runner.setProperty(ConsumeKafka.CLIENT_ID, "foo");

        try {
            runner.assertValid();
            fail();
        } catch (AssertionError e) {
            assertTrue(e.getMessage().contains("must contain at least one character that is not white space"));
        }

        runner.setProperty(ConsumeKafka.BOOTSTRAP_SERVERS, "foo");
        try {
            runner.assertValid();
            fail();
        } catch (AssertionError e) {
            assertTrue(e.getMessage().contains("'bootstrap.servers' validated against 'foo' is invalid"));
        }
        runner.setProperty(ConsumeKafka.BOOTSTRAP_SERVERS, "foo:1234");

        runner.removeProperty(ConsumeKafka.TOPIC);
        try {
            runner.assertValid();
            fail();
        } catch (AssertionError e) {
            assertTrue(e.getMessage().contains("'topic' is invalid because topic is required"));
        }

        runner.setProperty(ConsumeKafka.TOPIC, "");
        try {
            runner.assertValid();
            fail();
        } catch (AssertionError e) {
            assertTrue(e.getMessage().contains("must contain at least one character that is not white space"));
        }

        runner.setProperty(ConsumeKafka.TOPIC, "  ");
        try {
            runner.assertValid();
            fail();
        } catch (AssertionError e) {
            assertTrue(e.getMessage().contains("must contain at least one character that is not white space"));
        }
        runner.setProperty(ConsumeKafka.TOPIC, "blah");

        runner.removeProperty(ConsumeKafka.CLIENT_ID);
        try {
            runner.assertValid();
            fail();
        } catch (AssertionError e) {
            assertTrue(e.getMessage().contains("invalid because client.id is required"));
        }

        runner.setProperty(ConsumeKafka.CLIENT_ID, "");
        try {
            runner.assertValid();
            fail();
        } catch (AssertionError e) {
            assertTrue(e.getMessage().contains("must contain at least one character that is not white space"));
        }

        runner.setProperty(ConsumeKafka.CLIENT_ID, "   ");
        try {
            runner.assertValid();
            fail();
        } catch (AssertionError e) {
            assertTrue(e.getMessage().contains("must contain at least one character that is not white space"));
        }
        runner.setProperty(ConsumeKafka.CLIENT_ID, "ghj");

        runner.setProperty(PublishKafka.KERBEROS_PRINCIPLE, "");
        try {
            runner.assertValid();
            fail();
        } catch (AssertionError e) {
            assertTrue(e.getMessage().contains("must contain at least one character that is not white space"));
        }
        runner.setProperty(PublishKafka.KERBEROS_PRINCIPLE, "  ");
        try {
            runner.assertValid();
            fail();
        } catch (AssertionError e) {
            assertTrue(e.getMessage().contains("must contain at least one character that is not white space"));
        }
    }

    @Test
    @Ignore // just for extra sanity check
    public void validateConcurrencyWithRandomFailuresMultiple() throws Exception {
        for (int i = 0; i < 100; i++) {
            validateConcurrencyWithRandomFailures();
        }
    }

    @Test
    public void validateConcurrencyWithRandomFailures() throws Exception {
        ExecutorService processingExecutor = Executors.newFixedThreadPool(32);
        final AtomicInteger commitCounter = new AtomicInteger();
        final AtomicInteger rollbackCounter = new AtomicInteger();
        final AtomicInteger yieldCounter = new AtomicInteger();

        final ProcessSessionFactory sessionFactory = mock(ProcessSessionFactory.class);
        final ProcessSession session = mock(ProcessSession.class);
        when(sessionFactory.createSession()).thenReturn(session);
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                commitCounter.incrementAndGet();
                return null;
            }
        }).when(session).commit();
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                rollbackCounter.incrementAndGet();
                return null;
            }
        }).when(session).rollback(true);

        final ConcurrencyValidatingProcessor processor = spy(new ConcurrencyValidatingProcessor());

        int testCount = 1000;
        final CountDownLatch latch = new CountDownLatch(testCount);
        for (int i = 0; i < testCount; i++) {
            processingExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    ProcessContext context = mock(ProcessContext.class);
                    doAnswer(new Answer<Void>() {
                        @Override
                        public Void answer(InvocationOnMock invocation) throws Throwable {
                            yieldCounter.incrementAndGet();
                            return null;
                        }
                    }).when(context).yield();
                    if (random.nextInt(10) == 5) {
                        when(context.getName()).thenReturn("fail");
                    }
                    try {
                        processor.onTrigger(context, sessionFactory);
                    } catch (Exception e) {
                        fail();
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }

        assertTrue(latch.await(20000, TimeUnit.MILLISECONDS));
        processingExecutor.shutdown();

        System.out.println("SUCCESS: " + processor.successfulTriggers);
        System.out.println("FAILURE: " + processor.failedTriggers);
        System.out.println("INIT: " + processor.resourceReinitialized);
        System.out.println("YIELD CALLS: " + yieldCounter.get());
        System.out.println("COMMIT CALLS: " + commitCounter.get());
        System.out.println("ROLLBACK CALLS: " + rollbackCounter.get());
        System.out.println("Close CALLS: " + processor.closeCounter.get());

        /*
         * this has to be <= 1 since the last thread may come to finally block
         * after acceptTask flag has been reset at which point the close will
         * not be called (which is correct behavior since it will be invoked
         * explicitly by the life-cycle operations of a running processor).
         *
         * You can actually observe the =1 behavior in the next test where it is
         * always 0 close calls
         */
        int closeVsInitDiff = processor.resourceReinitialized.get() - processor.closeCounter.get();
        assertTrue(closeVsInitDiff <= 1);

        assertEquals(commitCounter.get(), processor.successfulTriggers.get());
        assertEquals(rollbackCounter.get(), processor.failedTriggers.get());

        assertEquals(testCount,
                processor.successfulTriggers.get() + processor.failedTriggers.get() + yieldCounter.get());
    }

    @Test
    public void validateConcurrencyWithAllSuccesses() throws Exception {
        ExecutorService processingExecutor = Executors.newFixedThreadPool(32);
        final AtomicInteger commitCounter = new AtomicInteger();
        final AtomicInteger rollbackCounter = new AtomicInteger();
        final AtomicInteger yieldCounter = new AtomicInteger();

        final ProcessSessionFactory sessionFactory = mock(ProcessSessionFactory.class);
        final ProcessSession session = mock(ProcessSession.class);
        when(sessionFactory.createSession()).thenReturn(session);
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                commitCounter.incrementAndGet();
                return null;
            }
        }).when(session).commit();
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                rollbackCounter.incrementAndGet();
                return null;
            }
        }).when(session).rollback(true);

        final ConcurrencyValidatingProcessor processor = spy(new ConcurrencyValidatingProcessor());

        int testCount = 1000;
        final CountDownLatch latch = new CountDownLatch(testCount);
        for (int i = 0; i < testCount; i++) {
            processingExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    ProcessContext context = mock(ProcessContext.class);
                    doAnswer(new Answer<Void>() {
                        @Override
                        public Void answer(InvocationOnMock invocation) throws Throwable {
                            yieldCounter.incrementAndGet();
                            return null;
                        }
                    }).when(context).yield();
                    try {
                        processor.onTrigger(context, sessionFactory);
                    } catch (Exception e) {
                        fail();
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }

        assertTrue(latch.await(30000, TimeUnit.MILLISECONDS));
        processingExecutor.shutdown();

        System.out.println("SUCCESS: " + processor.successfulTriggers);
        System.out.println("FAILURE: " + processor.failedTriggers);
        System.out.println("INIT: " + processor.resourceReinitialized);
        System.out.println("YIELD CALLS: " + yieldCounter.get());
        System.out.println("COMMIT CALLS: " + commitCounter.get());
        System.out.println("ROLLBACK CALLS: " + rollbackCounter.get());
        System.out.println("Close CALLS: " + processor.closeCounter.get());

        /*
         * unlike previous test this one will always be 1 since there are no
         * failures
         */
        int closeVsInitDiff = processor.resourceReinitialized.get() - processor.closeCounter.get();
        assertEquals(1, closeVsInitDiff);

        assertEquals(commitCounter.get(), processor.successfulTriggers.get());
        assertEquals(rollbackCounter.get(), processor.failedTriggers.get());

        assertEquals(testCount,
                processor.successfulTriggers.get() + processor.failedTriggers.get() + yieldCounter.get());
    }

    @Test
    public void validateConcurrencyWithAllFailures() throws Exception {
        ExecutorService processingExecutor = Executors.newFixedThreadPool(32);
        final AtomicInteger commitCounter = new AtomicInteger();
        final AtomicInteger rollbackCounter = new AtomicInteger();
        final AtomicInteger yieldCounter = new AtomicInteger();

        final ProcessSessionFactory sessionFactory = mock(ProcessSessionFactory.class);
        final ProcessSession session = mock(ProcessSession.class);
        when(sessionFactory.createSession()).thenReturn(session);
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                commitCounter.incrementAndGet();
                return null;
            }
        }).when(session).commit();
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                rollbackCounter.incrementAndGet();
                return null;
            }
        }).when(session).rollback(true);

        final ConcurrencyValidatingProcessor processor = spy(new ConcurrencyValidatingProcessor());

        int testCount = 1000;
        final CountDownLatch latch = new CountDownLatch(testCount);
        for (int i = 0; i < testCount; i++) {
            processingExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    ProcessContext context = mock(ProcessContext.class);
                    doAnswer(new Answer<Void>() {
                        @Override
                        public Void answer(InvocationOnMock invocation) throws Throwable {
                            yieldCounter.incrementAndGet();
                            return null;
                        }
                    }).when(context).yield();
                    when(context.getName()).thenReturn("fail");
                    try {
                        processor.onTrigger(context, sessionFactory);
                    } catch (Exception e) {
                        fail();
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }

        assertTrue(latch.await(20000, TimeUnit.MILLISECONDS));
        processingExecutor.shutdown();

        System.out.println("SUCCESS: " + processor.successfulTriggers);
        System.out.println("FAILURE: " + processor.failedTriggers);
        System.out.println("INIT: " + processor.resourceReinitialized);
        System.out.println("YIELD CALLS: " + yieldCounter.get());
        System.out.println("COMMIT CALLS: " + commitCounter.get());
        System.out.println("ROLLBACK CALLS: " + rollbackCounter.get());
        System.out.println("Close CALLS: " + processor.closeCounter.get());

        /*
         * unlike previous test this one will always be 0 since all triggers are
         * failures
         */
        int closeVsInitDiff = processor.resourceReinitialized.get() - processor.closeCounter.get();
        assertEquals(0, closeVsInitDiff);

        assertEquals(commitCounter.get(), processor.successfulTriggers.get());
        assertEquals(rollbackCounter.get(), processor.failedTriggers.get());

        assertEquals(testCount,
                processor.successfulTriggers.get() + processor.failedTriggers.get() + yieldCounter.get());
    }

    /**
     *
     */
    public static class DummyProcessor extends AbstractKafkaProcessor<Closeable> {
        @Override
        protected boolean rendezvousWithKafka(ProcessContext context, ProcessSession session) throws ProcessException {
            return true;
        }

        @Override
        protected Closeable buildKafkaResource(ProcessContext context, ProcessSession session) throws ProcessException {
            return mock(Closeable.class);
        }

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            return SHARED_DESCRIPTORS;
        }
    }


    public static class ConcurrencyValidatingProcessor extends AbstractKafkaProcessor<Closeable> {
        final AtomicInteger failedTriggers = new AtomicInteger();
        final AtomicInteger successfulTriggers = new AtomicInteger();
        final AtomicInteger resourceReinitialized = new AtomicInteger();
        final AtomicInteger closeCounter = new AtomicInteger();

        ConcurrencyValidatingProcessor() {
            try {
                Field loggerField = AbstractSessionFactoryProcessor.class.getDeclaredField("logger");
                loggerField.setAccessible(true);
                loggerField.set(this, mock(ComponentLog.class));
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        @OnStopped
        public void close() {
            super.close();
            assertTrue(this.kafkaResource == null);
            closeCounter.incrementAndGet();
        }

        @Override
        protected boolean rendezvousWithKafka(ProcessContext context, ProcessSession session) {
            assertNotNull(this.kafkaResource);
            if ("fail".equals(context.getName())) {
                failedTriggers.incrementAndGet();
                throw new RuntimeException("Intentional");
            }
            this.successfulTriggers.incrementAndGet();
            return true;
        }

        @Override
        protected Closeable buildKafkaResource(ProcessContext context, ProcessSession session) throws ProcessException {
            this.resourceReinitialized.incrementAndGet();
            return mock(Closeable.class);
        }
    }
}
