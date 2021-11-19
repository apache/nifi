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
package org.apache.nifi.processors.limiter;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.record.path.validation.RecordPathValidator;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.expression.ExpressionLanguageScope;

import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket4j;
import io.github.bucket4j.*;

import com.google.common.cache.*;

@Tags({ "record", "rate limiter", "flooding", "record" })
@CapabilityDescription("Provide a system to limit the incoming traffic in nifi. Each record will be limited using the provided key that can be define using a record path.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({
        @WritesAttribute(attribute = "record.count", description = "The FlowFile will have a 'record.count' attribute indicating the number of records "
            + "that were written to the FlowFile."),
        @WritesAttribute(attribute = "record.flooding", description = "The FlowFile will have a 'record.flooding' attribute indicating the number of records "
            + "that were considered provider flooding the system"),
        @WritesAttribute(attribute = "mime.type", description = "The MIME Type indicated by the Record Writer")
})
public class RateLimiterRecord extends AbstractProcessor implements RateLimiterProcessor {

        static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
                .name("put-es-record-reader")
                .displayName("Record Reader")
                .description("The record reader to use for reading incoming records from flowfiles.")
                .identifiesControllerService(RecordReaderFactory.class)
                .required(true)
                .build();

        static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
                .name("record-writer")
                .displayName("Record Writer")
                .description("The Record Writer to use in order to serialize the data before sending to Kafka")
                .identifiesControllerService(RecordSetWriterFactory.class)
                .expressionLanguageSupported(ExpressionLanguageScope.NONE)
                .required(true)
                .build();

        static final PropertyDescriptor KEY_RECORD_PATH = new PropertyDescriptor.Builder()
                .name("put-key-record-path")
                .displayName("Key Record Path")
                .description("A record path expression to retrieve the key field for use with rate limiter. If left blank " +
                        "the key will be determined using the main key property.")
                .addValidator(new RecordPathValidator())
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .build();

        static final PropertyDescriptor KEY_REMOVED = new PropertyDescriptor.Builder()
                .name("put-key-removed")
                .displayName("Key Removed")
                .description("Decide if the record key path  used for rate limiter should be removed or not"
                 + "from the records.")
                .required(true)
                .allowableValues("true", "false")
                .defaultValue("false")
                .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
                .build();

        static final List<PropertyDescriptor> DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
                RECORD_READER, RECORD_WRITER, KEY, KEY_RECORD_PATH, KEY_REMOVED, REFILL_TOKENS, REFILL_PERIOD, BANDWIDTH_CAPACITY, MAX_SIZE_BUCKET, EXPIRE_DURATION
            ));

        static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
                REL_SUCCESS, REL_FAILURE, REL_FLOODING
            )));

        @Override
        public Set<Relationship> getRelationships() {
                return RELATIONSHIPS;
        }

        @Override
        public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
                return DESCRIPTORS;
        }

        private RecordReaderFactory readerFactory;
        private RecordPathCache recordPathCache;
        private RecordSetWriterFactory writerFactory;
        private LoadingCache < String, Bucket > internalCache = null;
        private boolean key_removed; 

        @OnScheduled
        public void onScheduled(final ProcessContext context) {
                this.readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
                this.writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
                this.recordPathCache = new RecordPathCache(16);
                this.key_removed = context.getProperty(KEY_REMOVED).asBoolean();

                try {
                        this.internalCache = CacheBuilder.newBuilder()
                            .maximumSize(Integer.parseInt(context.getProperty(MAX_SIZE_BUCKET).getValue()))
                            .expireAfterWrite(context.getProperty(EXPIRE_DURATION).asTimePeriod(TimeUnit.MILLISECONDS).intValue(), TimeUnit.MILLISECONDS) 
                            .build(
                                new CacheLoader < String, Bucket > () {
                                    public Bucket load(String key) {
                                        Refill refill = Refill.intervally(Integer.parseInt(context.getProperty(REFILL_TOKENS).getValue()),Duration.ofMillis(context.getProperty(REFILL_PERIOD).asTimePeriod(TimeUnit.MILLISECONDS).intValue()));
                                        Bandwidth limit = Bandwidth.classic(Integer.parseInt(context.getProperty(BANDWIDTH_CAPACITY).getValue()), refill);
                                        return Bucket4j.builder().addLimit(limit).build();
                                    }
                                });
                } catch (Exception e) {
                        getLogger().error("Could not intiate InternalCache", e);
                }
        }
        
        @OnUnscheduled
        public void onUnScheduled(final ProcessContext context) {
                if ( this.internalCache != null ){
                        this.internalCache.cleanUp();
                        this.internalCache = null;
                }
        }


        @Override
        public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
                FlowFile input = session.get();
                if ( input == null ) {
                        return;
                }

                final String key = context.getProperty(KEY).evaluateAttributeExpressions(input).getValue(); 
                final String keyPath = context.getProperty(KEY_RECORD_PATH).isSet()
                        ? context.getProperty(KEY_RECORD_PATH).evaluateAttributeExpressions(input).getValue()
                        : null;

                RecordPath _keyPath = keyPath != null ? recordPathCache.getCompiled(keyPath) : null;

                FlowFile output = session.create(input);
                FlowFile flooding = session.create(input); 
                FlowFile failure = session.create(input); 

                try (InputStream inStream = session.read(input);
                     OutputStream os = session.write(output);
                     OutputStream osFail = session.write(failure);
                     OutputStream osFlooding = session.write(flooding) ) {

                        RecordReader reader = readerFactory.createRecordReader(input, inStream, getLogger());
                        RecordSchema schema = writerFactory.getSchema(input.getAttributes(), reader.getSchema());
                        RecordSetWriter writer = writerFactory.createWriter(getLogger(), schema, os);
                        RecordSetWriter failWriter = writerFactory.createWriter(getLogger(), schema, osFail);
                        RecordSetWriter floodingWriter = writerFactory.createWriter(getLogger(), schema, osFlooding);
                        Record record;
                        writer.beginRecordSet();
                        failWriter.beginRecordSet();
                        floodingWriter.beginRecordSet();

                        int count = 0;
                        int floodingCount = 0;
                        int failCount = 0; 
                        while ((record = reader.nextRecord()) != null) {
                                final String bucketKey = getFromRecordPath(record, _keyPath, key);
                                Bucket mybucket = this.internalCache.get(bucketKey);

                                if (mybucket != null) {
                                        //try to consume
                                        ConsumptionProbe probe = mybucket.tryConsumeAndReturnRemaining(1);
                                
                                        if (probe.isConsumed()) {
                                                // RateLimiterRes: "OK"
                                                count++; 
                                                writer.write(record);
                                        } else {
                                                // RateLimiterRes: "KO"
                                                floodingCount++; 
                                                floodingWriter.write(record); 
                                        }
                                } else {
                                        // RateLimiterRes: "Mybucket Null"
                                        getLogger().error("Rate Limiter : Mybucket Null"); 
                                        failCount++; 
                                        failWriter.write(record);
                                }
                        }
                        
                        writer.finishRecordSet();
                        writer.close();
                        failWriter.finishRecordSet();
                        failWriter.close();
                        floodingWriter.finishRecordSet();
                        floodingWriter.close();
                        inStream.close();
                        os.close();
                        osFail.close();
                        osFlooding.close();

                        if ( count > 0 ){
                                output = session.putAttribute(output, ATTR_MIME_TYPE, "application/json");
                                output = session.putAttribute(output, ATTR_RECORD_COUNT, String.valueOf(count));
                                output = session.putAttribute(output, ATTR_RECORD_COUNT_FLOODING, String.valueOf(floodingCount));
                                session.transfer(output, REL_SUCCESS);
                        } else {
                                session.remove(output);
                        }
                        if ( floodingCount > 0 ){
                                flooding = session.putAttribute(flooding, ATTR_MIME_TYPE, "application/json");
                                flooding = session.putAttribute(flooding, ATTR_RECORD_COUNT, String.valueOf(floodingCount));
                                session.transfer(flooding, REL_FLOODING);
                        } else {
                            
                                session.remove(flooding);
                        }
                        if ( failCount > 0 ){
                                failure = session.putAttribute(failure, ATTR_MIME_TYPE, "application/json");
                                failure = session.putAttribute(failure, ATTR_RECORD_COUNT, String.valueOf(failCount));
                                session.transfer(failure, REL_FAILURE);
                        } else {
                                session.remove(failure);
                        }
                        session.remove(input);

                } catch (Exception ex) {
                        getLogger().error("Could not check internal cache.", ex);
                        session.transfer(input, REL_FAILURE);
                        return;
                }
        }

        private String getFromRecordPath(Record record, RecordPath path, final String fallback) {
                if (path == null) {
                    return fallback;
                }
        
                RecordPathResult result = path.evaluate(record);
                Optional<FieldValue> value = result.getSelectedFields().findFirst();
                if ( value.isPresent() ) {
                        FieldValue fieldValue = value.get();
                        Object val = fieldValue.getValue();
                        if (val == null) {
                                return fallback;
                        }
                        if (this.key_removed){
                                fieldValue.updateValue(null);
                        }
                        String realValue = val.toString();
        
                        return realValue;
                } else {
                        return fallback;
                }
        }

}
