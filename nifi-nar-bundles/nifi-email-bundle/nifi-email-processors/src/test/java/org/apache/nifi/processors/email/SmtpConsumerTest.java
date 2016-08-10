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
package org.apache.nifi.processors.email;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.IOUtils;
import org.apache.commons.mail.Email;
import org.apache.commons.mail.SimpleEmail;
import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.subethamail.smtp.server.SMTPServer;

public class SmtpConsumerTest {

    private volatile ExecutorService executor;

    @Before
    public void before() {
        this.executor = Executors.newCachedThreadPool();
    }

    @After
    public void after() {
        this.executor.shutdown();
    }

    @Test
    public void validateServerCanStopWhenConsumerStoppedBeforeConsumingMessage() throws Exception {
        SmtpConsumer consumer = new SmtpConsumer();
        CountDownLatch latch = new CountDownLatch(10);
        AtomicReference<Exception> exception = new AtomicReference<>();
        this.executor.execute(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 10; i++) {
                    try {
                        consumer.data(mock(InputStream.class));
                    } catch (Exception e) {
                        e.printStackTrace();
                        exception.set(e);
                    } finally {
                        latch.countDown();
                    }
                }
            }
        });
        boolean finished = latch.await(2000, TimeUnit.MILLISECONDS);
        assertFalse(finished);
        this.executor.shutdown();
        boolean terminated = this.executor.awaitTermination(1000, TimeUnit.MILLISECONDS);
        assertFalse(terminated);

        consumer.stop();
        finished = latch.await(1000, TimeUnit.MILLISECONDS);
        assertTrue(finished);
        terminated = this.executor.awaitTermination(1000, TimeUnit.MILLISECONDS);
        assertTrue(terminated);
    }

    /*
     * This test simply validates that consumeUsing(..) can react properly to
     * thread interrupts. That said the condition is impossible in the current
     * usage of SmtpConsumer
     */
    @Test
    public void validateServerCanStopWhenConsumerInterrupted() throws Exception {
        SmtpConsumer consumer = new SmtpConsumer();
        AtomicReference<Thread> thread = new AtomicReference<>();

        this.executor.execute(new Runnable() {
            @Override
            public void run() {
                thread.set(Thread.currentThread());
                consumer.consumeUsing((in) -> {
                    return 0;
                });
            }
        });

        this.executor.shutdown();
        boolean terminated = this.executor.awaitTermination(200, TimeUnit.MILLISECONDS);
        assertFalse(terminated); // the call to consumeUsing(..) is blocking on
                                 // the queue.poll since nothing is there

        thread.get().interrupt(); // interrupt thread that executes
                                  // consumeUsing(..)
        terminated = this.executor.awaitTermination(1000, TimeUnit.MILLISECONDS);
        assertTrue(terminated);
    }

    /*
     * Emulates any errors that may arise while reading the {@link InputStream}
     * delivered as part of the data() call.
     */
    @Test
    public void validateServerCanStopWhenConsumerErrors() throws Exception {
        SmtpConsumer consumer = new SmtpConsumer();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> exception = new AtomicReference<>();
        this.executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    consumer.data(mock(InputStream.class));
                } catch (Exception e) {
                    exception.set(e);
                } finally {
                    latch.countDown();
                }
            }
        });

        this.executor.execute(new Runnable() {
            @Override
            public void run() {
                consumer.consumeUsing((in) -> {
                    throw new RuntimeException("intentional");
                });
            }
        });

        // this to ensure that call to data unblocks
        assertTrue(latch.await(1000, TimeUnit.MILLISECONDS));
        assertTrue(exception.get() instanceof IllegalStateException);
        assertEquals("Consuming thread failed while processing 'data' SMTP event.", exception.get().getMessage());
    }

    @Test
    public void validateServerCanStopWithUnconsumedMessage() throws Exception {
        int port = NetworkUtils.availablePort();
        SmtpConsumer consumer = new SmtpConsumer();
        SMTPServer smtp = new SMTPServer(consumer);
        smtp.setPort(port);
        smtp.setSoftwareName("Apache NiFi");
        smtp.start();

        BlockingQueue<Exception> exceptionQueue = new ArrayBlockingQueue<>(1);
        this.executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    Email email = new SimpleEmail();
                    email.setHostName("localhost");
                    email.setSmtpPort(port);
                    email.setFrom("alice@nifi.apache.org");
                    email.setSubject("This is a test");
                    email.setMsg("Hello SMTP");
                    email.addTo("bob@nifi.apache.org");
                    email.send();
                } catch (Exception e) {
                    exceptionQueue.offer(e);
                }
            }
        });
        assertNull(exceptionQueue.poll());
        smtp.stop();
        Exception ex = exceptionQueue.poll(100, TimeUnit.MILLISECONDS);
        assertNotNull(ex);
        /*
         * This essentially ensures and validates that if NiFi was not able to
         * successfully consume message the aftermath of the exception thrown by
         * the consumer is propagated to the sender essentially ensuring no data
         * loss by allowing sender to resend
         */
        assertTrue(ex.getMessage().startsWith("Sending the email to the following server failed"));
    }

    @Test
    public void validateConsumer() throws Exception {
        int port = NetworkUtils.availablePort();
        SmtpConsumer consumer = new SmtpConsumer();
        SMTPServer smtp = new SMTPServer(consumer);
        smtp.setPort(port);
        smtp.setSoftwareName("Apache NiFi");
        smtp.start();

        int messageCount = 5;
        CountDownLatch latch = new CountDownLatch(messageCount);
        this.executor.execute(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < messageCount; i++) {
                    try {
                        Email email = new SimpleEmail();
                        email.setHostName("localhost");
                        email.setSmtpPort(port);
                        email.setFrom("alice@nifi.apache.org");
                        email.setSubject("This is a test");
                        email.setMsg("MSG-" + i);
                        email.addTo("bob@nifi.apache.org");
                        email.send();
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                }
            }
        });

        List<String> messages = new ArrayList<>();
        for (AtomicInteger i = new AtomicInteger(); i.get() < messageCount;) {
            consumer.consumeUsing((dataInputStream) -> {
                i.incrementAndGet();
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                int size = 0;
                try {
                    size = IOUtils.copy(dataInputStream, bos);
                    messages.add(new String(bos.toByteArray(), StandardCharsets.UTF_8));
                    return size;
                } catch (Exception e) {
                    e.printStackTrace();
                    fail();
                }
                return size;
            });
        }

        boolean complete = latch.await(5000, TimeUnit.MILLISECONDS);
        assertTrue(complete);
        assertTrue(messages.size() == messageCount);
        assertTrue(messages.get(0).contains("MSG-0"));
        assertTrue(messages.get(1).contains("MSG-1"));
        assertTrue(messages.get(2).contains("MSG-2"));
        assertTrue(messages.get(3).contains("MSG-3"));
        assertTrue(messages.get(4).contains("MSG-4"));
        smtp.stop();
    }
}
