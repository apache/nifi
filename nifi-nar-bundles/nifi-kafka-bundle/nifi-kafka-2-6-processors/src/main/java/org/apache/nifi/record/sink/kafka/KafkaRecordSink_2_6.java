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
package org.apache.nifi.record.sink.kafka;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.kafka.pubsub.KafkaProcessorUtils;
import org.apache.nifi.record.sink.RecordSinkService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.stream.io.ByteCountingOutputStream;
import org.apache.nifi.stream.io.exception.TokenTooLargeException;
import org.apache.nifi.util.FormatUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


@Tags({"kafka", "record", "sink"})
@CapabilityDescription("Provides a service to write records to a Kafka 2.6+ topic.")
public class KafkaRecordSink_2_6 extends AbstractControllerService implements RecordSinkService {

    static final AllowableValue DELIVERY_REPLICATED = new AllowableValue("all", "Guarantee Replicated Delivery",
            "Records are considered 'transmitted unsuccessfully' unless the message is replicated to the appropriate "
                    + "number of Kafka Nodes according to the Topic configuration.");
    static final AllowableValue DELIVERY_ONE_NODE = new AllowableValue("1", "Guarantee Single Node Delivery",
            "Records are considered 'transmitted successfully' if the message is received by a single Kafka node, "
                    + "whether or not it is replicated. This is faster than <Guarantee Replicated Delivery> "
                    + "but can result in data loss if a Kafka node crashes.");
    static final AllowableValue DELIVERY_BEST_EFFORT = new AllowableValue("0", "Best Effort",
            "Records are considered 'transmitted successfully' after successfully writing the content to a Kafka node, "
                    + "without waiting for a response. This provides the best performance but may result in data loss.");

    static final AllowableValue UTF8_ENCODING = new AllowableValue("utf-8", "UTF-8 Encoded", "The key is interpreted as a UTF-8 Encoded string.");
    static final AllowableValue HEX_ENCODING = new AllowableValue("hex", "Hex Encoded",
            "The key is interpreted as arbitrary binary data that is encoded using hexadecimal characters with uppercase letters.");

    static final PropertyDescriptor TOPIC = new PropertyDescriptor.Builder()
            .name("topic")
            .displayName("Topic Name")
            .description("The name of the Kafka Topic to publish to.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor DELIVERY_GUARANTEE = new PropertyDescriptor.Builder()
            .name("acks")
            .displayName("Delivery Guarantee")
            .description("Specifies the requirement for guaranteeing that a message is sent to Kafka. Corresponds to Kafka's 'acks' property.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(DELIVERY_BEST_EFFORT, DELIVERY_ONE_NODE, DELIVERY_REPLICATED)
            .defaultValue(DELIVERY_BEST_EFFORT.getValue())
            .build();

    static final PropertyDescriptor METADATA_WAIT_TIME = new PropertyDescriptor.Builder()
            .name("max.block.ms")
            .displayName("Max Metadata Wait Time")
            .description("The amount of time publisher will wait to obtain metadata or wait for the buffer to flush during the 'send' call before failing the "
                    + "entire 'send' call. Corresponds to Kafka's 'max.block.ms' property")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("5 sec")
            .build();

    static final PropertyDescriptor ACK_WAIT_TIME = new PropertyDescriptor.Builder()
            .name("ack.wait.time")
            .displayName("Acknowledgment Wait Time")
            .description("After sending a message to Kafka, this indicates the amount of time that we are willing to wait for a response from Kafka. "
                    + "If Kafka does not acknowledge the message within this time period, the FlowFile will be routed to 'failure'.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .defaultValue("5 secs")
            .build();

    static final PropertyDescriptor MAX_REQUEST_SIZE = new PropertyDescriptor.Builder()
            .name("max.request.size")
            .displayName("Max Request Size")
            .description("The maximum size of a request in bytes. Corresponds to Kafka's 'max.request.size' property and defaults to 1 MB (1048576).")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("1 MB")
            .build();

    static final PropertyDescriptor COMPRESSION_CODEC = new PropertyDescriptor.Builder()
            .name("compression.type")
            .displayName("Compression Type")
            .description("This parameter allows you to specify the compression codec for all data generated by this producer.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues("none", "gzip", "snappy", "lz4")
            .defaultValue("none")
            .build();

    static final PropertyDescriptor MESSAGE_HEADER_ENCODING = new PropertyDescriptor.Builder()
            .name("message-header-encoding")
            .displayName("Message Header Encoding")
            .description("For any attribute that is added as a message header, as configured via the <Attributes to Send as Headers> property, "
                    + "this property indicates the Character Encoding to use for serializing the headers.")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue("UTF-8")
            .required(false)
            .build();

    private List<PropertyDescriptor> properties;
    private volatile RecordSetWriterFactory writerFactory;
    private volatile int maxMessageSize;
    private volatile long maxAckWaitMillis;
    private volatile String topic;
    private volatile Producer<byte[], byte[]> producer;

    @Override
    protected void init(final ControllerServiceInitializationContext context) throws InitializationException {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(KafkaProcessorUtils.BOOTSTRAP_SERVERS);
        properties.add(TOPIC);
        properties.add(RecordSinkService.RECORD_WRITER_FACTORY);
        properties.add(DELIVERY_GUARANTEE);
        properties.add(MESSAGE_HEADER_ENCODING);
        properties.add(KafkaProcessorUtils.SECURITY_PROTOCOL);
        properties.add(KafkaProcessorUtils.KERBEROS_CREDENTIALS_SERVICE);
        properties.add(KafkaProcessorUtils.JAAS_SERVICE_NAME);
        properties.add(KafkaProcessorUtils.SSL_CONTEXT_SERVICE);
        properties.add(MAX_REQUEST_SIZE);
        properties.add(ACK_WAIT_TIME);
        properties.add(METADATA_WAIT_TIME);
        properties.add(COMPRESSION_CODEC);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .description("Specifies the value for '" + propertyDescriptorName + "' Kafka Configuration.")
                .name(propertyDescriptorName)
                .addValidator(new KafkaProcessorUtils.KafkaConfigValidator(ProducerConfig.class))
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        return KafkaProcessorUtils.validateCommonProperties(validationContext);
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        topic = context.getProperty(TOPIC).evaluateAttributeExpressions().getValue();
        writerFactory = context.getProperty(RecordSinkService.RECORD_WRITER_FACTORY).asControllerService(RecordSetWriterFactory.class);
        maxMessageSize = context.getProperty(MAX_REQUEST_SIZE).asDataSize(DataUnit.B).intValue();
        maxAckWaitMillis = context.getProperty(ACK_WAIT_TIME).asTimePeriod(TimeUnit.MILLISECONDS);

        final String charsetName = context.getProperty(MESSAGE_HEADER_ENCODING).evaluateAttributeExpressions().getValue();
        final Charset charset = Charset.forName(charsetName);

        final Map<String, Object> kafkaProperties = new HashMap<>();
        buildCommonKafkaProperties(context, ProducerConfig.class, kafkaProperties);
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProperties.put("max.request.size", String.valueOf(maxMessageSize));

        try {
            producer = createProducer(kafkaProperties);
        } catch (Exception e) {
            getLogger().error("Could not create Kafka producer due to {}", new Object[]{e.getMessage()}, e);
            throw new InitializationException(e);
        }
    }

    @Override
    public WriteResult sendData(final RecordSet recordSet, final Map<String, String> attributes, final boolean sendZeroResults) throws IOException {

        try {
            WriteResult writeResult;
            final RecordSchema writeSchema = getWriterFactory().getSchema(null, recordSet.getSchema());
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final ByteCountingOutputStream out = new ByteCountingOutputStream(baos);
            int recordCount = 0;
            try (final RecordSetWriter writer = getWriterFactory().createWriter(getLogger(), writeSchema, out, attributes)) {
                writer.beginRecordSet();
                Record record;
                while ((record = recordSet.next()) != null) {
                    writer.write(record);
                    recordCount++;
                    if (out.getBytesWritten() > maxMessageSize) {
                        throw new TokenTooLargeException("The query's result set size exceeds the maximum allowed message size of " + maxMessageSize + " bytes.");
                    }
                }
                writeResult = writer.finishRecordSet();
                if (out.getBytesWritten() > maxMessageSize) {
                    throw new TokenTooLargeException("The query's result set size exceeds the maximum allowed message size of " + maxMessageSize + " bytes.");
                }
                recordCount = writeResult.getRecordCount();

                attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                attributes.put("record.count", Integer.toString(recordCount));
                attributes.putAll(writeResult.getAttributes());
            }

            if (recordCount > 0 || sendZeroResults) {
                final ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, null, null, baos.toByteArray());
                try {
                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            throw new KafkaSendException(exception);
                        }
                    }).get(maxAckWaitMillis, TimeUnit.MILLISECONDS);
                } catch (KafkaSendException kse) {
                    Throwable t = kse.getCause();
                    if (t instanceof IOException) {
                        throw (IOException) t;
                    } else {
                        throw new IOException(t);
                    }
                } catch (final InterruptedException e) {
                    getLogger().warn("Interrupted while waiting for an acknowledgement from Kafka");
                    Thread.currentThread().interrupt();
                } catch (final TimeoutException e) {
                    getLogger().warn("Timed out while waiting for an acknowledgement from Kafka");
                }
            } else {
                writeResult = WriteResult.EMPTY;
            }

            return writeResult;
        } catch (IOException ioe) {
            throw ioe;
        } catch (Exception e) {
            throw new IOException("Failed to write metrics using record writer: " + e.getMessage(), e);
        }

    }

    @OnDisabled
    public void stop() throws IOException {
        if (producer != null) {
            producer.close(maxAckWaitMillis, TimeUnit.MILLISECONDS);
        }
    }

    static void buildCommonKafkaProperties(final ConfigurationContext context, final Class<?> kafkaConfigClass, final Map<String, Object> mapToPopulate) {
        for (PropertyDescriptor propertyDescriptor : context.getProperties().keySet()) {
            if (propertyDescriptor.equals(KafkaProcessorUtils.SSL_CONTEXT_SERVICE)) {
                // Translate SSLContext Service configuration into Kafka properties
                final SSLContextService sslContextService = context.getProperty(KafkaProcessorUtils.SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
                if (sslContextService != null && sslContextService.isKeyStoreConfigured()) {
                    mapToPopulate.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, sslContextService.getKeyStoreFile());
                    mapToPopulate.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, sslContextService.getKeyStorePassword());
                    final String keyPass = sslContextService.getKeyPassword() == null ? sslContextService.getKeyStorePassword() : sslContextService.getKeyPassword();
                    mapToPopulate.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPass);
                    mapToPopulate.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, sslContextService.getKeyStoreType());
                }

                if (sslContextService != null && sslContextService.isTrustStoreConfigured()) {
                    mapToPopulate.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslContextService.getTrustStoreFile());
                    mapToPopulate.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, sslContextService.getTrustStorePassword());
                    mapToPopulate.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, sslContextService.getTrustStoreType());
                }
            }

            String propertyName = propertyDescriptor.getName();
            String propertyValue = propertyDescriptor.isExpressionLanguageSupported()
                    ? context.getProperty(propertyDescriptor).evaluateAttributeExpressions().getValue()
                    : context.getProperty(propertyDescriptor).getValue();

            if (propertyValue != null) {
                // If the property name ends in ".ms" then it is a time period. We want to accept either an integer as number of milliseconds
                // or the standard NiFi time period such as "5 secs"
                if (propertyName.endsWith(".ms") && !StringUtils.isNumeric(propertyValue.trim())) { // kafka standard time notation
                    propertyValue = String.valueOf(FormatUtils.getTimeDuration(propertyValue.trim(), TimeUnit.MILLISECONDS));
                }

                if (KafkaProcessorUtils.isStaticStringFieldNamePresent(propertyName, kafkaConfigClass, CommonClientConfigs.class, SslConfigs.class, SaslConfigs.class)) {
                    mapToPopulate.put(propertyName, propertyValue);
                }
            }
        }

        String securityProtocol = context.getProperty(KafkaProcessorUtils.SECURITY_PROTOCOL).getValue();
        if (KafkaProcessorUtils.SEC_SASL_PLAINTEXT.getValue().equals(securityProtocol) || KafkaProcessorUtils.SEC_SASL_SSL.getValue().equals(securityProtocol)) {
            setJaasConfig(mapToPopulate, context);
        }
    }

    /**
     * Method used to configure the 'sasl.jaas.config' property based on KAFKA-4259<br />
     * https://cwiki.apache.org/confluence/display/KAFKA/KIP-85%3A+Dynamic+JAAS+configuration+for+Kafka+clients<br />
     * <br />
     * It expects something with the following format: <br />
     * <br />
     * &lt;LoginModuleClass&gt; &lt;ControlFlag&gt; *(&lt;OptionName&gt;=&lt;OptionValue&gt;); <br />
     * ControlFlag = required / requisite / sufficient / optional
     *
     * @param mapToPopulate Map of configuration properties
     * @param context       Context
     */
    private static void setJaasConfig(Map<String, Object> mapToPopulate, ConfigurationContext context) {
        String keytab = null;
        String principal = null;

        // If the Kerberos Credentials Service is specified, we need to use its configuration, not the explicit properties for principal/keytab.
        // The customValidate method ensures that only one can be set, so we know that the principal & keytab above are null.
        final KerberosCredentialsService credentialsService = context.getProperty(KafkaProcessorUtils.KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService.class);
        if (credentialsService != null) {
            principal = credentialsService.getPrincipal();
            keytab = credentialsService.getKeytab();
        }


        String serviceName = context.getProperty(KafkaProcessorUtils.JAAS_SERVICE_NAME).evaluateAttributeExpressions().getValue();
        if (StringUtils.isNotBlank(keytab) && StringUtils.isNotBlank(principal) && StringUtils.isNotBlank(serviceName)) {
            mapToPopulate.put(SaslConfigs.SASL_JAAS_CONFIG, "com.sun.security.auth.module.Krb5LoginModule required "
                    + "useTicketCache=false "
                    + "renewTicket=true "
                    + "serviceName=\"" + serviceName + "\" "
                    + "useKeyTab=true "
                    + "keyTab=\"" + keytab + "\" "
                    + "principal=\"" + principal + "\";");
        }
    }

    // this getter is intended explicitly for testing purposes
    protected RecordSetWriterFactory getWriterFactory() {
        return this.writerFactory;
    }

    protected Producer<byte[], byte[]> createProducer(Map<String, Object> kafkaProperties) {
        return new KafkaProducer<>(kafkaProperties);
    }

    private static class KafkaSendException extends RuntimeException {
        KafkaSendException(Throwable cause) {
            super(cause);
        }
    }
}
