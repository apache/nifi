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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.mockito.Mockito;

import scala.util.Random;

public class PublishKafkaConditionEmulatingProcessor extends PublishKafka {

    private final AtomicInteger closeRequestsCount = new AtomicInteger();

    private final AtomicInteger failuresThatDoNotOwnCloseCount = new AtomicInteger();

    private final AtomicInteger intentionalFailures = new AtomicInteger();

    private final AtomicInteger kafkaResourceBuilt = new AtomicInteger();


    @SuppressWarnings("unchecked")
    @Override
    protected KafkaPublisher buildKafkaResource(ProcessContext context, ProcessSession session) throws ProcessException {
        kafkaResourceBuilt.incrementAndGet();
        KafkaPublisher kafkaPublisher = null;
        try {
            Producer<byte[], byte[]> kafkaProducer = mock(Producer.class);
            when(kafkaProducer.send(Mockito.any(ProducerRecord.class))).thenReturn(mock(Future.class));
            // Creates an instance of the class without invoking its constructor.
            kafkaPublisher = (KafkaPublisher) TestUtils.getUnsafe().allocateInstance(KafkaPublisher.class);
            Field f = kafkaPublisher.getClass().getDeclaredField("kafkaProducer");
            TestUtils.setFinalField(f, kafkaPublisher, kafkaProducer);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return kafkaPublisher;
    }



    @Override
    protected boolean rendezvousWithKafka(ProcessContext context, ProcessSession session) throws ProcessException {
        if (new Random().nextInt(10) > 5) {
            FlowFile flowFile = session.get();
            if (flowFile != null) {
                intentionalFailures.incrementAndGet();
                failuresThatDoNotOwnCloseCount.incrementAndGet();
                throw new RuntimeException("intentional");
            } else {
                context.yield();
                return true;
            }
        } else {
            return super.rendezvousWithKafka(context, session);
        }
    }

    @Override
    @OnStopped
    public void close() {
        this.closeRequestsCount.incrementAndGet();
        failuresThatDoNotOwnCloseCount.decrementAndGet();
        super.close();

    }

    public AtomicInteger getFailuresThatDoNotOwnCloseCount() {
        return failuresThatDoNotOwnCloseCount;
    }

    public AtomicInteger getCloseRequestsCount() {
        return closeRequestsCount;
    }

    public AtomicInteger getIntentionalFailuresCount() {
        return intentionalFailures;
    }

    public AtomicInteger getKafkaResourceBuiltCount() {
        return kafkaResourceBuilt;
    }
}
