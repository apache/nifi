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
package org.apache.nifi.atlas.emulator;

import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.notification.MessageDeserializer;
import org.apache.atlas.notification.NotificationInterface;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.function.Consumer;

public class AtlasNotificationServerEmulator {

    // EntityPartialUpdateRequest
    // EntityCreateRequest
    // NotificationInterface


    private volatile boolean isStopped;

    public void consume(Consumer<HookNotification> c) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("ATLAS_HOOK"));

        isStopped = false;
        while (!isStopped) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                final MessageDeserializer deserializer = NotificationInterface.NotificationType.HOOK.getDeserializer();
                final HookNotification m
                        = (HookNotification) deserializer.deserialize(record.value());
                c.accept(m);
            }
        }

        consumer.close();
    }

    public void stop() {
        isStopped = true;
    }
}
