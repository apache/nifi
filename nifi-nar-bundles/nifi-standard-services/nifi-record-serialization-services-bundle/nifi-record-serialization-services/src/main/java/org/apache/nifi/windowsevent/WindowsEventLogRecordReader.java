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
package org.apache.nifi.windowsevent;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.inference.TimeValueInference;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.stream.io.NonCloseableInputStream;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.xml.inference.XmlSchemaInference;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

public class WindowsEventLogRecordReader implements RecordReader {

    private final ComponentLog logger;
    private final RecordSchema schema;
    private boolean isArray = false;
    private XMLEventReader xmlEventReader;

    private StartElement currentRecordStartTag;
    // Utility used to infer <Data> tag data types
    private final XmlSchemaInference xmlSchemaInference;

    private final Supplier<DateFormat> LAZY_DATE_FORMAT;
    private final Supplier<DateFormat> LAZY_TIME_FORMAT;
    private final Supplier<DateFormat> LAZY_TIMESTAMP_FORMAT;


    public WindowsEventLogRecordReader(InputStream in, final String dateFormat, final String timeFormat, final String timestampFormat, ComponentLog logger)
            throws IOException, MalformedRecordException {

        this.logger = logger;

        final DateFormat df = dateFormat == null ? null : DataTypeUtils.getDateFormat(dateFormat);
        final DateFormat tf = timeFormat == null ? null : DataTypeUtils.getDateFormat(timeFormat);
        final DateFormat tsf = timestampFormat == null ? null : DataTypeUtils.getDateFormat(timestampFormat);

        LAZY_DATE_FORMAT = () -> df;
        LAZY_TIME_FORMAT = () -> tf;
        LAZY_TIMESTAMP_FORMAT = () -> tsf;

        FilterInputStream inputStream;
        try {
            final XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
            // Avoid XXE Vulnerabilities
            xmlInputFactory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
            xmlInputFactory.setProperty("javax.xml.stream.isSupportingExternalEntities", false);

            inputStream = new NonCloseableInputStream(in);
            inputStream.mark(Integer.MAX_VALUE);
            xmlEventReader = xmlInputFactory.createXMLEventReader(inputStream);
            xmlSchemaInference = new XmlSchemaInference(new TimeValueInference(dateFormat, timeFormat, timestampFormat));

            // Do a streaming pass through the input looking for <Data> tags, then reset the input
            schema = determineSchema();

            // Restart the XML event stream and advance to the first Event tag
            inputStream.reset();
            xmlEventReader = xmlInputFactory.createXMLEventReader(inputStream);
            if (isArray) {
                skipToNextStartTag();
            }
            setNextRecordStartTag();
        } catch (XMLStreamException e) {
            throw new MalformedRecordException("Could not parse XML", e);
        }
    }

    @Override
    public Record nextRecord(final boolean coerceTypes, final boolean dropUnknownFields) throws IOException, MalformedRecordException {
        if (currentRecordStartTag == null) {
            return null;
        }
        try {
            final Record record = parseRecord(currentRecordStartTag, this.schema, coerceTypes, dropUnknownFields);
            setNextRecordStartTag();
            if (record != null) {
                return record;
            } else {
                return new MapRecord(this.schema, Collections.emptyMap());
            }
        } catch (XMLStreamException e) {
            throw new MalformedRecordException("Could not parse XML", e);
        }
    }

    @Override
    public RecordSchema getSchema() throws MalformedRecordException {
        return schema;
    }

    @Override
    public void close() throws IOException {
        try {
            xmlEventReader.close();
        } catch (XMLStreamException e) {
            logger.error("Unable to close XMLEventReader");
        }
    }

    private RecordSchema determineSchema() throws XMLStreamException {

        setNextRecordStartTag();
        if (currentRecordStartTag == null) {
            throw new XMLStreamException("No root tag found, must be one of <Events> or <Event>");
        }

        if (currentRecordStartTag.getName().getLocalPart().equals("Events")) {
            isArray = true;
            setNextRecordStartTag();
        }

        // If there was an <Events></Events> tag pair with no Events in it, use the default schema
        if (currentRecordStartTag == null) {
            return generateFullSchema(new SimpleRecordSchema(Collections.emptyList()));
        }

        List<RecordField> dataFields = new ArrayList<>();
        List<String> dataFieldNames = new ArrayList<>();
        while (currentRecordStartTag != null) {
            if (!currentRecordStartTag.getName().getLocalPart().equals("Event")) {
                // Unknown and invalid tag
                throw new XMLStreamException("Expecting <Event> tag but found unknown/invalid tag " + currentRecordStartTag.getName().getLocalPart());
            }

            setNextRecordStartTag();
            // At an Event tag, skip the event log type tag (System, e.g.), go into EventData tag then add all the Data tags to the partial schema
            while (currentRecordStartTag != null && !currentRecordStartTag.getName().getLocalPart().equals("EventData")) {
                skipElement();
                setNextRecordStartTag();
            }

            if (currentRecordStartTag == null) {
                throw new XMLStreamException("Expecting <EventData> tag but found none");
            }

            setNextRecordStartTag();
            if (currentRecordStartTag == null) {
                // There was an <EventData></EventData> tag but no Data/Binary tags, so this record has been fully processed
                continue;
            }

            String eventDataElementName = currentRecordStartTag.getName().getLocalPart();
            while ("Data" .equals(eventDataElementName)) {
                // Save reference to Data start element so we can continue to get the value/content
                StartElement dataElement = currentRecordStartTag;
                String content = getContent();

                // Create field for the data point using attribute "Name"
                String dataFieldName;
                final Iterator<?> iterator = dataElement.getAttributes();
                if (!iterator.hasNext()) {
                    // If no Name attribute is provided, the Name is the content of the Data tag and there should be a following Binary tag
                    dataFieldName = content;
                    setNextRecordStartTag();
                    eventDataElementName = currentRecordStartTag.getName().getLocalPart();
                    if (!"Binary" .equals(eventDataElementName)) {
                        throw new XMLStreamException("Expecting <Binary> tag containing data for element: " + dataFieldName);
                    }
                    content = getContent();

                } else {
                    final Attribute attribute = (Attribute) iterator.next();
                    final String attributeName = attribute.getName().getLocalPart();
                    if (!"Name" .equals(attributeName)) {
                        throw new XMLStreamException("Expecting 'Name' attribute, actual: " + attributeName);
                    }
                    dataFieldName = attribute.getValue();
                }
                // Skip this if it has been processed in a previous record
                if (!dataFieldNames.contains(dataFieldName)) {
                    final DataType dataElementDataType = xmlSchemaInference.inferTextualDataType(content);
                    RecordField newRecordField = new RecordField(dataFieldName, dataElementDataType, true);
                    dataFields.add(newRecordField);
                    dataFieldNames.add(dataFieldName);
                }

                // Advance to next data point (or end of EventData)
                setNextRecordStartTag();
                eventDataElementName = currentRecordStartTag == null ? null : currentRecordStartTag.getName().getLocalPart();
            }
        }

        return generateFullSchema(new SimpleRecordSchema(dataFields));
    }

    private void skipElement() throws XMLStreamException {
        while (xmlEventReader.hasNext()) {
            final XMLEvent xmlEvent = xmlEventReader.nextEvent();

            if (xmlEvent.isStartElement()) {
                skipElement();
            }
            if (xmlEvent.isEndElement()) {
                return;
            }
        }
    }

    private void skipToNextStartTag() throws XMLStreamException {
        while (xmlEventReader.hasNext()) {
            final XMLEvent xmlEvent = xmlEventReader.nextEvent();
            if (xmlEvent.isStartElement()) {
                return;
            }
        }
    }

    private void setNextRecordStartTag() throws XMLStreamException {
        while (xmlEventReader.hasNext()) {
            final XMLEvent xmlEvent = xmlEventReader.nextEvent();
            if (xmlEvent.isStartElement()) {
                currentRecordStartTag = xmlEvent.asStartElement();
                return;
            }
        }
        currentRecordStartTag = null;
    }

    private RecordSchema generateFullSchema(final RecordSchema dataElementsSchema) {
        final SimpleRecordSchema rootSchema;

        // Generate the full (input) schema even if the output schema is flattened
        List<RecordField> systemProviderFields = new ArrayList<>();
        systemProviderFields.add(new RecordField("Guid", RecordFieldType.STRING.getDataType(), false));
        systemProviderFields.add(new RecordField("Name", RecordFieldType.STRING.getDataType(), false));
        SimpleRecordSchema systemProviderSchema = new SimpleRecordSchema(systemProviderFields);
        systemProviderSchema.setSchemaName("Provider");

        List<RecordField> systemTimeCreatedFields = new ArrayList<>(1);
        systemTimeCreatedFields.add(new RecordField("SystemTime", RecordFieldType.STRING.getDataType(), false));
        SimpleRecordSchema systemTimeCreatedSchema = new SimpleRecordSchema(systemTimeCreatedFields);
        systemTimeCreatedSchema.setSchemaName("TimeCreated");

        List<RecordField> systemExecutionFields = new ArrayList<>(2);
        systemExecutionFields.add(new RecordField("ThreadID", RecordFieldType.INT.getDataType(), true));
        systemExecutionFields.add(new RecordField("ProcessID", RecordFieldType.INT.getDataType(), true));
        SimpleRecordSchema systemExecutionSchema = new SimpleRecordSchema(systemExecutionFields);
        systemExecutionSchema.setSchemaName("Execution");

        List<RecordField> systemFields = new ArrayList<>(14);
        systemFields.add(new RecordField("Provider", RecordFieldType.RECORD.getRecordDataType(systemProviderSchema)));
        systemFields.add(new RecordField("EventID", RecordFieldType.INT.getDataType(), true));
        systemFields.add(new RecordField("Version", RecordFieldType.INT.getDataType(), true));
        systemFields.add(new RecordField("Level", RecordFieldType.INT.getDataType(), true));
        systemFields.add(new RecordField("Task", RecordFieldType.INT.getDataType(), true));
        systemFields.add(new RecordField("Opcode", RecordFieldType.INT.getDataType(), true));
        systemFields.add(new RecordField("Keywords", RecordFieldType.STRING.getDataType(), true));
        systemFields.add(new RecordField("TimeCreated", RecordFieldType.RECORD.getRecordDataType(systemTimeCreatedSchema)));
        systemFields.add(new RecordField("EventRecordID", RecordFieldType.INT.getDataType(), true));
        systemFields.add(new RecordField("Correlation", RecordFieldType.STRING.getDataType(), true));
        systemFields.add(new RecordField("Execution", RecordFieldType.RECORD.getRecordDataType(systemExecutionSchema)));
        systemFields.add(new RecordField("Channel", RecordFieldType.STRING.getDataType(), true));
        systemFields.add(new RecordField("Computer", RecordFieldType.STRING.getDataType(), true));
        systemFields.add(new RecordField("Security", RecordFieldType.STRING.getDataType(), true));

        SimpleRecordSchema systemSchema = new SimpleRecordSchema(systemFields);
        systemSchema.setSchemaName("System");

        List<RecordField> fullSchemaFields = new ArrayList<>(2);
        fullSchemaFields.add(new RecordField("System", RecordFieldType.RECORD.getRecordDataType(systemSchema)));
        fullSchemaFields.add(new RecordField("EventData", RecordFieldType.RECORD.getRecordDataType(dataElementsSchema)));

        SimpleRecordSchema fullSchema = new SimpleRecordSchema(fullSchemaFields);
        fullSchema.setSchemaName("Event");

        rootSchema = fullSchema;
        return rootSchema;
    }

    private String getContent() throws XMLStreamException {
        StringBuilder content = new StringBuilder();
        while (xmlEventReader.hasNext()) {
            XMLEvent xmlEvent = xmlEventReader.nextEvent();
            if (xmlEvent.isCharacters()) {
                final Characters characters = xmlEvent.asCharacters();
                if (!characters.isWhiteSpace()) {
                    content.append(characters.getData());
                }
            } else if (xmlEvent.isEndElement()) {
                break;
            } else if (xmlEvent.isStartElement()) {
                this.skipElement();
            }
        }
        return content.toString();
    }

    private Record parseRecord(StartElement startElement, RecordSchema schema, boolean coerceTypes, boolean dropUnknown) throws XMLStreamException, MalformedRecordException {
        final Map<String, Object> recordValues = new HashMap<>();

        // parse attributes
        final Iterator<?> iterator = startElement.getAttributes();
        while (iterator.hasNext()) {
            final Attribute attribute = (Attribute) iterator.next();
            final String targetFieldName = attribute.getName().toString();

            if (dropUnknown) {
                final Optional<RecordField> field = schema.getField(targetFieldName);
                if (field.isPresent()) {

                    // dropUnknown == true && coerceTypes == true
                    if (coerceTypes) {
                        final Object value;
                        final DataType dataType = field.get().getDataType();
                        if ((value = parseStringForType(attribute.getValue(), targetFieldName, dataType)) != null) {
                            recordValues.put(targetFieldName, value);
                        }

                        // dropUnknown == true && coerceTypes == false
                    } else {
                        recordValues.put(targetFieldName, attribute.getValue());
                    }
                }
            } else {

                // dropUnknown == false && coerceTypes == true
                if (coerceTypes) {
                    final Object value;
                    final Optional<RecordField> field = schema.getField(targetFieldName);
                    if (field.isPresent()) {
                        if ((value = parseStringForType(attribute.getValue(), targetFieldName, field.get().getDataType())) != null) {
                            recordValues.put(targetFieldName, value);
                        }
                    } else {
                        recordValues.put(targetFieldName, attribute.getValue());
                    }

                    // dropUnknown == false && coerceTypes == false
                } else {
                    recordValues.put(targetFieldName, attribute.getValue());
                }
            }
        }

        // parse fields
        while (xmlEventReader.hasNext()) {
            final XMLEvent xmlEvent = xmlEventReader.nextEvent();

            if (xmlEvent.isStartElement()) {
                final StartElement subStartElement = xmlEvent.asStartElement();
                String fieldName = subStartElement.getName().getLocalPart();

                final Optional<RecordField> field = schema.getField(fieldName);

                if (dropUnknown) {
                    if (field.isPresent()) {
                        // dropUnknown == true && coerceTypes == true
                        if (coerceTypes) {
                            final Object value = parseFieldForType(subStartElement, fieldName, field.get().getDataType(), recordValues, true);
                            if (value != null) {
                                recordValues.put(fieldName, value);
                            }

                            // dropUnknown == true && coerceTypes == false
                            // subElements of subStartElement can only be known if there is a corresponding field in the schema defined as record
                        }
                    } else {
                        if ("Data" .equals(fieldName)) {
                            Map<String, Object> namedValue = parseDataField(subStartElement, schema, true);
                            if(namedValue != null && !namedValue.isEmpty()) {
                                final String name = namedValue.keySet().iterator().next();
                                recordValues.put(name, namedValue.get(name));
                            }
                        } else {
                            skipElement();
                        }
                    }
                } else {
                    // dropUnknown == false && coerceTypes == true
                    if (coerceTypes) {
                        if (field.isPresent()) {
                            final Object value = parseFieldForType(subStartElement, fieldName, field.get().getDataType(), recordValues, false);
                            if (value != null) {
                                recordValues.put(fieldName, value);
                            }
                        } else {
                            if ("Data" .equals(fieldName)) {
                                Map<String, Object> namedValue = parseDataField(subStartElement, schema, dropUnknown);
                                if(namedValue != null && !namedValue.isEmpty()) {
                                    final String name = namedValue.keySet().iterator().next();
                                    recordValues.put(name, namedValue.get(name));
                                }
                            }
                        }

                        // dropUnknown == false && coerceTypes == false
                    } else {
                        if ("Data" .equals(fieldName)) {
                            Map<String, Object> namedValue = parseDataField(subStartElement, schema, dropUnknown);
                            if(namedValue != null && !namedValue.isEmpty()) {
                                final String name = namedValue.keySet().iterator().next();
                                recordValues.put(name, namedValue.get(name));
                            }
                        }
                    }
                }
            } else if (xmlEvent.isEndElement()) {
                break;
            }
        }

        for (final Map.Entry<String, Object> entry : recordValues.entrySet()) {
            if (entry.getValue() instanceof List) {
                recordValues.put(entry.getKey(), ((List) entry.getValue()).toArray());
            }
        }

        if (recordValues.size() > 0) {
            return new MapRecord(schema, recordValues);
        } else {
            return null;
        }
    }

    private Object parseStringForType(String data, String fieldName, DataType dataType) {
        switch (dataType.getFieldType()) {
            case BOOLEAN:
            case BYTE:
            case CHAR:
            case DECIMAL:
            case DOUBLE:
            case FLOAT:
            case INT:
            case LONG:
            case SHORT:
            case STRING:
            case DATE:
            case TIME:
            case TIMESTAMP: {
                return DataTypeUtils.convertType(data, dataType, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT, fieldName);
            }
        }
        return null;
    }

    private Object parseFieldForType(StartElement startElement, String fieldName, DataType dataType, Map<String, Object> recordValues,
                                     boolean dropUnknown) throws XMLStreamException, MalformedRecordException {
        switch (dataType.getFieldType()) {
            case BOOLEAN:
            case BYTE:
            case CHAR:
            case DECIMAL:
            case DOUBLE:
            case FLOAT:
            case INT:
            case LONG:
            case SHORT:
            case STRING:
            case DATE:
            case TIME:
            case TIMESTAMP: {

                StringBuilder content = new StringBuilder();

                while (xmlEventReader.hasNext()) {
                    XMLEvent xmlEvent = xmlEventReader.nextEvent();
                    if (xmlEvent.isCharacters()) {
                        final Characters characters = xmlEvent.asCharacters();
                        if (!characters.isWhiteSpace()) {
                            content.append(characters.getData());
                        }
                    } else if (xmlEvent.isEndElement()) {
                        final String contentToReturn = content.toString();

                        if (!StringUtils.isBlank(contentToReturn)) {
                            return DataTypeUtils.convertType(content.toString(), dataType, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT, fieldName);
                        } else {
                            return null;
                        }

                    } else if (xmlEvent.isStartElement()) {
                        this.skipElement();
                    }
                }
                break;
            }

            case ARRAY: {
                final DataType arrayDataType = ((ArrayDataType) dataType).getElementType();

                final Object newValue = parseFieldForType(startElement, fieldName, arrayDataType, recordValues, dropUnknown);
                final Object oldValues = recordValues.get(fieldName);

                if (newValue != null) {
                    if (oldValues != null) {
                        if (oldValues instanceof List) {
                            ((List) oldValues).add(newValue);
                        } else {
                            List<Object> arrayValues = new ArrayList<>();
                            arrayValues.add(oldValues);
                            arrayValues.add(newValue);
                            return arrayValues;
                        }
                    } else {
                        List<Object> arrayValues = new ArrayList<>();
                        arrayValues.add(newValue);
                        return arrayValues;
                    }
                }
                return oldValues;
            }

            case RECORD: {
                final RecordSchema childSchema;
                if (dataType instanceof RecordDataType) {
                    childSchema = ((RecordDataType) dataType).getChildSchema();
                } else {
                    return null;
                }

                return parseRecord(startElement, childSchema, true, dropUnknown);
            }

            case MAP: {
                final DataType mapDataType = ((MapDataType) dataType).getValueType();
                final Map<String, Object> embeddedMap = new HashMap<>();

                while (xmlEventReader.hasNext()) {
                    XMLEvent xmlEvent = xmlEventReader.nextEvent();

                    if (xmlEvent.isStartElement()) {
                        final StartElement subStartElement = xmlEvent.asStartElement();
                        final String subFieldName = subStartElement.getName().getLocalPart();

                        final Object mapValue = parseFieldForType(subStartElement, subFieldName, mapDataType, embeddedMap, dropUnknown);
                        embeddedMap.put(subFieldName, mapValue);

                    } else if (xmlEvent.isEndElement()) {
                        break;
                    }
                }

                if (embeddedMap.size() > 0) {
                    return embeddedMap;
                } else {
                    return null;
                }
            }
        }
        return null;
    }

    private Map<String, Object> parseDataField(StartElement startElement, RecordSchema schema, boolean dropUnknown) throws XMLStreamException {
        // Save reference to Data start element so we can continue to get the value/content
        String content = getContent();

        // Create field for the data point using attribute "Name"
        String dataFieldName;
        final Iterator<?> iterator = startElement.getAttributes();
        if (!iterator.hasNext()) {
            // If no Name attribute is provided, the Name is the content of the Data tag and there should be a following Binary tag
            dataFieldName = content;
            setNextRecordStartTag();
            String eventDataElementName = currentRecordStartTag.getName().getLocalPart();
            if (!"Binary" .equals(eventDataElementName)) {
                throw new XMLStreamException("Expecting <Binary> tag containing data for element: " + dataFieldName);
            }
            content = getContent();

        } else {
            final Attribute attribute = (Attribute) iterator.next();
            final String attributeName = attribute.getName().getLocalPart();
            if (!"Name" .equals(attributeName)) {
                throw new XMLStreamException("Expecting 'Name' attribute, actual: " + attributeName);
            }
            dataFieldName = attribute.getValue();
        }

        Optional<RecordField> rf = schema.getField(dataFieldName);
        if (rf.isPresent()) {
            return Collections.singletonMap(dataFieldName,
                    DataTypeUtils.convertType(content, rf.get().getDataType(), LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT, dataFieldName));
        } else if (dropUnknown) {
            return Collections.emptyMap();
        } else {
            return Collections.singletonMap(dataFieldName, content);
        }
    }
}

