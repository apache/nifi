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
package org.apache.nifi.record.sink;

import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.util.MockPropertyConfiguration;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestEmailRecordSink {

    /**
     * Extension of EmailRecordSink that stubs out the calls to
     * Transport.sendMessage().
     *
     * <p>
     * All sent messages are records in a list available via the
     * {@link #getMessages()} method.</p>
     * <p> Calling
     * {@link #setException(MessagingException)} will cause the supplied exception to be
     * thrown when sendMessage is invoked.
     * </p>
     */
    private static final class EmailRecordSinkExtension extends EmailRecordSink {
        private MessagingException e;
        private final List<Message> messages = new ArrayList<>();

        @Override
        protected void send(Message msg) throws MessagingException {
            messages.add(msg);
            if (this.e != null) {
                throw e;
            }
        }

        void setException(final MessagingException e) {
            this.e = e;
        }

        List<Message> getMessages() {
            return messages;
        }
    }

    private EmailRecordSinkExtension recordSink;
    private RecordSet recordSet;

    @BeforeEach
    public void setup() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        runner.setValidateExpressionUsage(false);

        recordSink = new EmailRecordSinkExtension();
        runner.addControllerService("emailRecordSink", recordSink);

        MockRecordWriter writerFactory = new MockRecordWriter();
        runner.addControllerService("writer", writerFactory);
        runner.setProperty(recordSink, EmailRecordSink.RECORD_WRITER_FACTORY, "writer");
        runner.setProperty(recordSink, EmailRecordSink.SMTP_HOSTNAME, "localhost");
        runner.setProperty(recordSink, EmailRecordSink.FROM, "sender@test.com");
        runner.setProperty(recordSink, EmailRecordSink.TO, "receiver@test.com");

        runner.enableControllerService(writerFactory);
        runner.enableControllerService(recordSink);


        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("a", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("b", RecordFieldType.BOOLEAN.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> valueMap1 = new HashMap<>();
        valueMap1.put("a", "Hello");
        valueMap1.put("b", true);
        final Record record1 = new MapRecord(schema, valueMap1);

        final Map<String, Object> valueMap2 = new HashMap<>();
        valueMap2.put("a", "World");
        valueMap2.put("b", false);
        final Record record2 = new MapRecord(schema, valueMap2);

        recordSet = RecordSet.of(schema, record1, record2);
    }

    @Test
    public void testRecordIsSentByEmail() throws IOException, MessagingException {
        final WriteResult writeResult = recordSink.sendData(recordSet, Collections.emptyMap(), false);
        final List<Message> messages = recordSink.getMessages();
        final String expectedMessage = "\"Hello\",\"true\"\n\"World\",\"false\"\n";
        assertNotNull(writeResult);
        assertEquals(2, writeResult.getRecordCount());
        assertEquals(1, messages.size());
        assertEquals(expectedMessage, messages.getFirst().getDataHandler().getContent());
    }

    @Test
    public void testSendEmailThrowsException() {
        final String exceptionMessage = "test exception message";
        recordSink.setException(new MessagingException(exceptionMessage));
        final RuntimeException runtimeException = assertThrows(RuntimeException.class,
                () -> recordSink.sendData(recordSet, Collections.emptyMap(), false)
        );
        assertTrue(runtimeException.getMessage().contains("localhost"));
        assertTrue(runtimeException.getMessage().contains("25"));
        assertEquals(exceptionMessage, runtimeException.getCause().getMessage());
    }

    @Test
    void testMigrateProperties() {
        final Map<String, String> expectedRenamed = Map.ofEntries(
                Map.entry("from", EmailRecordSink.FROM.getName()),
                Map.entry("to", EmailRecordSink.TO.getName()),
                Map.entry("cc", EmailRecordSink.CC.getName()),
                Map.entry("bcc", EmailRecordSink.BCC.getName()),
                Map.entry("subject", EmailRecordSink.SUBJECT.getName()),
                Map.entry("smtp-hostname", EmailRecordSink.SMTP_HOSTNAME.getName()),
                Map.entry("smtp-port", EmailRecordSink.SMTP_PORT.getName()),
                Map.entry("smtp-auth", EmailRecordSink.SMTP_AUTH.getName()),
                Map.entry("smtp-username", EmailRecordSink.SMTP_USERNAME.getName()),
                Map.entry("smtp-password", EmailRecordSink.SMTP_PASSWORD.getName()),
                Map.entry("smtp-starttls", EmailRecordSink.SMTP_STARTTLS.getName()),
                Map.entry("smtp-ssl", EmailRecordSink.SMTP_SSL.getName()),
                Map.entry("smtp-xmailer-header", EmailRecordSink.HEADER_XMAILER.getName()),
                Map.entry("record-sink-record-writer", RecordSinkService.RECORD_WRITER_FACTORY.getName())
        );

        final Map<String, String> propertyValues = Map.of();
        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(propertyValues);
        recordSink.migrateProperties(configuration);

        final PropertyMigrationResult result = configuration.toPropertyMigrationResult();
        final Map<String, String> propertiesRenamed = result.getPropertiesRenamed();

        assertEquals(expectedRenamed, propertiesRenamed);
    }
}

