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
package org.apache.nifi.processors;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"geo", "enrich", "ip", "maxmind", "record"})
@CapabilityDescription("Looks up geolocation information for an IP address and adds the geo information to FlowFile attributes. The "
        + "geo data is provided as a MaxMind database. This version uses the NiFi Record API to allow large scale enrichment of record-oriented data sets. "
        + "Each field provided by the MaxMind database can be directed to a field of the user's choosing by providing a record path for that field configuration. ")
public class GeoEnrichIPRecord extends AbstractEnrichIP {
    public static final PropertyDescriptor READER = new PropertyDescriptor.Builder()
            .name("geo-enrich-ip-record-reader")
            .displayName("Record Reader")
            .description("Record reader service to use for reading the flowfile contents.")
            .required(true)
            .identifiesControllerService(RecordReaderFactory.class)
            .build();
    public static final PropertyDescriptor WRITER = new PropertyDescriptor.Builder()
            .name("geo-enrich-ip-record-writer")
            .displayName("Record Writer")
            .description("Record writer service to use for enriching the flowfile contents.")
            .required(true)
            .identifiesControllerService(RecordSetWriterFactory.class)
            .build();
    public static final PropertyDescriptor IP_RECORD_PATH = new PropertyDescriptor.Builder()
            .name("geo-enrich-ip-ip-record-path")
            .displayName("IP Address Record Path")
            .description("The record path to retrieve the IP address for doing the lookup.")
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();
    public static final PropertyDescriptor SPLIT_FOUND_NOT_FOUND = new PropertyDescriptor.Builder()
            .name("geo-enrich-ip-split-found-not-found")
            .displayName("Separate Enriched From Not Enriched")
            .description("Separate records that have been enriched from ones that have not. Default behavior is " +
                    "to send everything to the found relationship if even one record is enriched.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor GEO_CITY = new PropertyDescriptor.Builder()
            .name("geo-enrich-ip-city-record-path")
            .displayName("City Record Path")
            .description("Record path for putting the city identified for the IP address")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor GEO_LATITUDE = new PropertyDescriptor.Builder()
            .name("geo-enrich-ip-latitude-record-path")
            .displayName("Latitude Record Path")
            .description("Record path for putting the latitude identified for this IP address")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor GEO_LONGITUDE = new PropertyDescriptor.Builder()
            .name("geo-enrich-ip-longitude-record-path")
            .displayName("Longitude Record Path")
            .description("Record path for putting the longitude identified for this IP address")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor GEO_COUNTRY = new PropertyDescriptor.Builder()
            .name("geo-enrich-ip-country-record-path")
            .displayName("Country Record Path")
            .description("Record path for putting the country identified for this IP address")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor GEO_COUNTRY_ISO = new PropertyDescriptor.Builder()
            .name("geo-enrich-ip-country-iso-record-path")
            .displayName("Country ISO Code Record Path")
            .description("Record path for putting the ISO Code for the country identified")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor GEO_POSTAL_CODE = new PropertyDescriptor.Builder()
            .name("geo-enrich-ip-country-postal-record-path")
            .displayName("Country Postal Code Record Path")
            .description("Record path for putting the postal code for the country identified")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original input flowfile goes to this relationship regardless of whether the content was enriched or not.")
            .build();

    public static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            REL_ORIGINAL, REL_FOUND, REL_NOT_FOUND
    )));

    public static final List<PropertyDescriptor> GEO_PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            GEO_CITY, GEO_LATITUDE, GEO_LONGITUDE, GEO_COUNTRY, GEO_COUNTRY_ISO, GEO_POSTAL_CODE
    ));

    private static final List<PropertyDescriptor> DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
            GEO_DATABASE_FILE, READER, WRITER, SPLIT_FOUND_NOT_FOUND, IP_RECORD_PATH, GEO_CITY, GEO_LATITUDE,
            GEO_LONGITUDE, GEO_COUNTRY, GEO_COUNTRY_ISO, GEO_POSTAL_CODE
    ));

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    protected volatile RecordReaderFactory readerFactory;
    protected volatile RecordSetWriterFactory writerFactory;
    protected boolean splitOutput;

    @Override
    @OnScheduled
    public void onScheduled(ProcessContext context) throws IOException {
        super.onScheduled(context);

        readerFactory = context.getProperty(READER).asControllerService(RecordReaderFactory.class);
        writerFactory = context.getProperty(WRITER).asControllerService(RecordSetWriterFactory.class);
        splitOutput = context.getProperty(SPLIT_FOUND_NOT_FOUND).asBoolean();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile inputFlowFile = session.get();
        if (inputFlowFile == null) {
            return;
        }

        FlowFile outputFlowFile = session.create(inputFlowFile);
        FlowFile notFoundFlowFile = splitOutput ? session.create(inputFlowFile) : null;
        final DatabaseReader dbReader = databaseReaderRef.get();
        try (final InputStream is = session.read(inputFlowFile);
             final OutputStream os = session.write(outputFlowFile);
             final OutputStream osNotFound = splitOutput ? session.write(notFoundFlowFile) : null) {
            final RecordPathCache recordPathCache = new RecordPathCache(GEO_PROPERTIES.size() + 1);
            final Map<PropertyDescriptor, RecordPath> recordPathMap = new HashMap<>();
            for (PropertyDescriptor descriptor : GEO_PROPERTIES) {
                if (!context.getProperty(descriptor).isSet()) {
                    continue;
                }

                final String rawPath = context.getProperty(descriptor).evaluateAttributeExpressions(inputFlowFile).getValue();
                final RecordPath compiled = recordPathCache.getCompiled(rawPath);
                recordPathMap.put(descriptor, compiled);
            }

            final String rawIpPath = context.getProperty(IP_RECORD_PATH).evaluateAttributeExpressions(inputFlowFile).getValue();
            final RecordPath ipPath = recordPathCache.getCompiled(rawIpPath);
            final RecordReader reader = readerFactory.createRecordReader(inputFlowFile, is, getLogger());
            final RecordSchema schema = writerFactory.getSchema(inputFlowFile.getAttributes(), reader.getSchema());
            final RecordSetWriter notFoundWriter = splitOutput ? writerFactory.createWriter(getLogger(), schema, osNotFound, inputFlowFile) : null;
            RecordSetWriter writer = null;
            Record record;
            Relationship targetRelationship = REL_NOT_FOUND;

            if (notFoundWriter != null) {
                notFoundWriter.beginRecordSet();
            }

            // For each flowfile record, enrich the IP address with geoip properties
            int foundCount = 0;
            int notFoundCount = 0;
            while ((record = reader.nextRecord()) != null) {
                // Enrich fields where the geo field descriptor is specified
                final CityResponse response = geocode(ipPath, record, dbReader);
                final boolean wasEnriched = enrichRecord(response, record, recordPathMap);

                // Record was enriched with one or more geo fields, so route it to the "found" relationship
                if (wasEnriched) {
                    targetRelationship = REL_FOUND;
                }

                if (!splitOutput || wasEnriched) {
                    // Initialise the writer, applying the enriched fields to the schema
                    if (writer == null) {
                        writer = writerFactory.createWriter(getLogger(), record.getSchema(), os, inputFlowFile);
                        writer.beginRecordSet();
                    }

                    writer.write(record);
                    foundCount++;
                } else if (notFoundWriter != null) {
                    notFoundWriter.write(record);
                    notFoundCount++;
                }
            }

            if (writer != null) {
                writer.finishRecordSet();
                writer.close();
            }
            if (notFoundWriter != null) {
                notFoundWriter.finishRecordSet();
                notFoundWriter.close();
            }

            is.close();
            os.close();
            if (osNotFound != null) {
                osNotFound.close();
            }

            if (writer != null) {
                outputFlowFile = session.putAllAttributes(outputFlowFile, buildAttributes(foundCount, writer.getMimeType()));
            }
            if (!splitOutput) {
                session.transfer(outputFlowFile, targetRelationship);
                session.remove(inputFlowFile);
            } else {
                if (notFoundCount > 0) {
                    if (writer != null) {
                        notFoundFlowFile = session.putAllAttributes(notFoundFlowFile, buildAttributes(notFoundCount, writer.getMimeType()));
                    }
                    session.transfer(notFoundFlowFile, REL_NOT_FOUND);
                } else {
                    session.remove(notFoundFlowFile);
                }
                if (foundCount > 0) {
                    session.transfer(outputFlowFile, REL_FOUND);
                } else {
                    session.remove(outputFlowFile);
                }

                session.transfer(inputFlowFile, REL_ORIGINAL);
                session.getProvenanceReporter().modifyContent(notFoundFlowFile);
            }

            session.getProvenanceReporter().modifyContent(outputFlowFile);
        } catch (Exception ex) {
            getLogger().error("Error enriching records.", ex);
            session.rollback();
            context.yield();
        }
    }

    private Map<String, String> buildAttributes(int recordCount, String mimeType) {
        final Map<String, String> retVal = new HashMap<>();
        retVal.put(CoreAttributes.MIME_TYPE.key(), mimeType);
        retVal.put("record.count", String.valueOf(recordCount));

        return retVal;
    }

    private CityResponse geocode(RecordPath ipPath, Record record, DatabaseReader reader) throws Exception {
        final RecordPathResult result = ipPath.evaluate(record);
        final Optional<FieldValue> ipField = result.getSelectedFields().findFirst();

        if (ipField.isPresent()) {
            final FieldValue value = ipField.get();
            final Object val = value.getValue();
            if (val == null) {
                return null;
            }
            final String realValue = val.toString();
            final InetAddress address = InetAddress.getByName(realValue);

            try {
                return reader.city(address);
            } catch (GeoIp2Exception ex) {
                getLogger().warn("Could not resolve the IP for value '{}' contained within the attribute '{}'. " +
                                "This is usually caused by the IP address not existing in the GeoIP database.",
                        new Object[]{realValue, IP_ADDRESS_ATTRIBUTE.getDisplayName()}, ex);
                return null;
            }
        } else {
            return null;
        }
    }

    private boolean enrichRecord(CityResponse response, Record record, Map<PropertyDescriptor, RecordPath> cached) {

        final boolean city = updateFieldValue(GEO_CITY, cached, record,
                response != null ? response.getCity().getName() : null, RecordFieldType.STRING);

        final boolean country = updateFieldValue(GEO_COUNTRY, cached, record,
                response != null ? response.getCountry().getName() : null, RecordFieldType.STRING);

        final boolean iso = updateFieldValue(GEO_COUNTRY_ISO, cached, record,
                response != null ? response.getCountry().getIsoCode() : null, RecordFieldType.STRING);

        final boolean lat = updateFieldValue(GEO_LATITUDE, cached, record,
                response != null ? response.getLocation().getLatitude() : null, RecordFieldType.DOUBLE);

        final boolean lon = updateFieldValue(GEO_LONGITUDE, cached, record,
                response != null ? response.getLocation().getLongitude() : null, RecordFieldType.DOUBLE);

        final boolean postal = updateFieldValue(GEO_POSTAL_CODE, cached, record,
                response != null ? response.getPostal().getCode() : null, RecordFieldType.STRING);

        // Ensure enriched fields are part of the record schema
        record.incorporateInactiveFields();

        return (city || country || iso || lat || lon || postal);
    }

    /**
     * Update the value of the target record field
     * @return Whether a non-null value was written to the record field
     */
    private boolean updateFieldValue(PropertyDescriptor descriptor, Map<PropertyDescriptor, RecordPath> cached, Record record,
                                     Object fieldValue, RecordFieldType fieldDataType) {

        // Skip if the target field path is not specified
        if (!cached.containsKey(descriptor)) {
            return false;
        }

        final RecordPath geoFieldPath = cached.get(descriptor);
        final RecordPathResult result = geoFieldPath.evaluate(record);
        final Optional<FieldValue> value = result.getSelectedFields().findFirst();

        value.ifPresent(val -> val.updateValue(fieldValue != null ? fieldValue.toString() : null,
                fieldDataType.getDataType()));

        return fieldValue != null;
    }
}
