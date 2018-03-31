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

package org.apache.nifi.xml;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
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

public class XMLRecordReader implements RecordReader {

    private final ComponentLog logger;
    private final RecordSchema schema;
    private final String recordName;
    private final String attributePrefix;
    private final String contentFieldName;

    // thread safety required?
    private StartElement currentRecordStartTag;

    private final XMLEventReader xmlEventReader;

    private final Supplier<DateFormat> LAZY_DATE_FORMAT;
    private final Supplier<DateFormat> LAZY_TIME_FORMAT;
    private final Supplier<DateFormat> LAZY_TIMESTAMP_FORMAT;

    public XMLRecordReader(InputStream in, RecordSchema schema, String rootName, String recordName, String attributePrefix, String contentFieldName,
                           final String dateFormat, final String timeFormat, final String timestampFormat, final ComponentLog logger) throws MalformedRecordException {
        this.schema = schema;
        this.recordName = recordName;
        this.attributePrefix = attributePrefix;
        this.contentFieldName = contentFieldName;
        this.logger = logger;

        final DateFormat df = dateFormat == null ? null : DataTypeUtils.getDateFormat(dateFormat);
        final DateFormat tf = timeFormat == null ? null : DataTypeUtils.getDateFormat(timeFormat);
        final DateFormat tsf = timestampFormat == null ? null : DataTypeUtils.getDateFormat(timestampFormat);

        LAZY_DATE_FORMAT = () -> df;
        LAZY_TIME_FORMAT = () -> tf;
        LAZY_TIMESTAMP_FORMAT = () -> tsf;

        try {
            final XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();

            // Avoid namespace replacements
            xmlInputFactory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, false);

            xmlEventReader = xmlInputFactory.createXMLEventReader(in);
            final StartElement rootTag = getNextStartTag();

            // root tag validation
            if (rootName != null && !rootName.equals(rootTag.getName().toString())) {
                final StringBuffer message = new StringBuffer();
                message.append("Name of root tag \"")
                        .append(rootTag.getName().toString())
                        .append("\" does not match root tag validation \"")
                        .append(rootName)
                        .append("\".");
                throw new MalformedRecordException(message.toString());
            }
            setNextRecordStartTag();
        } catch (XMLStreamException e) {
            throw new MalformedRecordException("Could not parse XML", e);
        }
    }

    private StartElement getNextStartTag() throws XMLStreamException {
        while (xmlEventReader.hasNext()) {
            final XMLEvent xmlEvent = xmlEventReader.nextEvent();
            if (xmlEvent.isStartElement()) {
                return xmlEvent.asStartElement();
            }
        }
        return null;
    }

    private void setNextRecordStartTag() throws XMLStreamException {
        while (xmlEventReader.hasNext()) {
            final XMLEvent xmlEvent = xmlEventReader.nextEvent();
            if (xmlEvent.isStartElement()) {
                final StartElement startElement = xmlEvent.asStartElement();
                if (recordName != null) {
                    if (startElement.getName().toString().equals(recordName)) {
                        currentRecordStartTag = startElement;
                        return;
                    } else {
                        logger.debug("Mismatch between expected record tag name {} and actual tag name in XML {}. " +
                                "Record will be skipped", new Object[] {recordName, startElement.getName().toString()});
                        skipElement();
                    }
                } else {
                    currentRecordStartTag = startElement;
                    return;
                }
            }
        }
        currentRecordStartTag = null;
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
                return new MapRecord(this.schema, Collections.EMPTY_MAP);
            }
        } catch (XMLStreamException e) {
            throw new MalformedRecordException("Could not parse XML", e);
        }
    }

    private Object parseFieldForType(StartElement startElement, String fieldName, DataType dataType, Map<String, Object> recordValues) throws XMLStreamException, MalformedRecordException {
        switch (dataType.getFieldType()) {
            case BOOLEAN:
            case BYTE:
            case CHAR:
            case DOUBLE:
            case FLOAT:
            case INT:
            case LONG:
            case SHORT:
            case STRING:
            case DATE:
            case TIME:
            case TIMESTAMP: {
                XMLEvent xmlEvent = xmlEventReader.nextEvent();
                if (xmlEvent.isCharacters()) {
                    final Characters characters = xmlEvent.asCharacters();
                    if (!characters.isWhiteSpace()) {
                        xmlEventReader.nextEvent();
                        return DataTypeUtils.convertType(characters.toString(), dataType, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT, fieldName);
                    } else {
                        xmlEvent = xmlEventReader.nextEvent();
                    }
                }
                if (xmlEvent.isEndElement()) {
                    return null;
                } else if (xmlEvent.isStartElement()) {
                    final String message = "Error parsing XML. Either the XML is invalid or there is a mismatch between schema type definitions and XML structure.";
                    throw new MalformedRecordException(message);
                }
            }
            case ARRAY: {
                final DataType arrayDataType = ((ArrayDataType) dataType).getElementType();

                final Object newValue = parseFieldForType(startElement, fieldName, arrayDataType, recordValues);
                final Object oldValues = recordValues.get(fieldName);

                if (newValue != null) {
                    if (oldValues != null) {
                        if (oldValues instanceof List) {
                            ((List) oldValues).add(newValue);
                        } else {
                            return new ArrayList<Object>(){{ add(oldValues); add(newValue); }};
                        }
                    } else {
                        return new ArrayList<Object>(){{ add(newValue); }};
                    }
                } else {
                    return null;
                }
            }
            case RECORD: {
                final RecordSchema childSchema;
                if (dataType instanceof RecordDataType) {
                    childSchema = ((RecordDataType) dataType).getChildSchema();
                } else {
                    return null;
                }
                return parseRecord(startElement, childSchema, true, true);
            }
            case MAP: {
                logger.warn("Type map is not supported by this record reader. Field will be skipped.");
                skipElement();
                return null;
            }
            case CHOICE: {
                return parseUnknownField(startElement);
            }
        }
        return null;
    }

    private Object parseUnknownField(StartElement startElement) throws XMLStreamException {
        // parse attributes
        final Map<String, Object> recordValues = new HashMap<>();
        final Iterator iterator = startElement.getAttributes();
        while (iterator.hasNext()) {
            final Attribute attribute = (Attribute) iterator.next();
            final String attributeName = attribute.getName().toString();
            recordValues.put(attributePrefix == null ? attributeName : attributePrefix + attributeName, attribute.getValue());
        }
        boolean hasAttributes = recordValues.size() > 0;

        // parse fields
        while (xmlEventReader.hasNext()) {
            final XMLEvent xmlEvent = xmlEventReader.nextEvent();
            if (xmlEvent.isCharacters()) {
                final Characters characters = xmlEvent.asCharacters();
                if (!characters.isWhiteSpace()) {
                    xmlEventReader.nextEvent();
                    if (hasAttributes) {
                        if (contentFieldName != null) {
                            recordValues.put(contentFieldName, characters.toString());
                        } else {
                            logger.debug("Found content for field that has to be parsed as record but property \"Field Name for Content\" is not set. " +
                                    "The content will not be added to the record.");
                        }
                        return new MapRecord(new SimpleRecordSchema(Collections.emptyList()), recordValues);
                    } else {
                        return characters.toString();
                    }
                }
            } else if (xmlEvent.isStartElement()){
                final StartElement subStartElement = xmlEvent.asStartElement();
                final String subFieldName = subStartElement.getName().toString();
                final Object value = parseUnknownField(subStartElement);

                if (value != null) {
                    putUnknownTypeInMap(recordValues, subFieldName, value);
                } else {
                    return null;
                }
            } else if (xmlEvent.isEndElement()) {
                break;
            }
        }

        for (final Map.Entry<String,Object> entry : recordValues.entrySet()) {
            if (entry.getValue() instanceof List) {
                recordValues.put(entry.getKey(), ((List) entry.getValue()).toArray());
            }
        }

        if (recordValues.size() > 0) {
            return new MapRecord(new SimpleRecordSchema(Collections.emptyList()), recordValues);
        } else {
            return null;
        }
    }

    private Record parseRecord(StartElement startElement, RecordSchema schema, boolean coerceTypes, boolean dropUnknown) throws XMLStreamException, MalformedRecordException {
        final Map<String, Object> recordValues = new HashMap<>();

        // parse attributes
        final Iterator iterator = startElement.getAttributes();
        while (iterator.hasNext()) {
            final Attribute attribute = (Attribute) iterator.next();
            final String attributeName = attribute.getName().toString();

            final String targetFieldName = attributePrefix == null ? attributeName : attributePrefix + attributeName;

            if (dropUnknown) {
                final Optional<RecordField> field = schema.getField(attributeName);
                if (field.isPresent()){

                    // dropUnknown == true && coerceTypes == true
                    if (coerceTypes) {
                        final Object value;
                        final DataType dataType = field.get().getDataType();
                        if ((value = parseAttributeForType(attribute, attributeName, dataType)) != null) {
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
                    final Optional<RecordField> field = schema.getField(attributeName);
                    if (field.isPresent()){
                        if ((value = parseAttributeForType(attribute, attributeName, field.get().getDataType())) != null) {
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
        while(xmlEventReader.hasNext()){
            final XMLEvent xmlEvent = xmlEventReader.nextEvent();

            if (xmlEvent.isStartElement()) {
                final StartElement subStartElement = xmlEvent.asStartElement();
                final String fieldName = subStartElement.getName().toString();
                final Optional<RecordField> field = schema.getField(fieldName);

                if (dropUnknown) {
                    if (field.isPresent()) {
                        // dropUnknown == true && coerceTypes == true
                        if (coerceTypes) {
                            final Object value = parseFieldForType(subStartElement, fieldName, field.get().getDataType(), recordValues);
                            if (value != null) {
                                recordValues.put(fieldName, value);
                            }

                        // dropUnknown == true && coerceTypes == false
                        } else {
                            final Object value = parseUnknownField(subStartElement);
                            if (value != null) {
                                putUnknownTypeInMap(recordValues, fieldName, value);
                            }
                        }

                    } else {
                        skipElement();
                    }
                } else {
                    // dropUnknown == false && coerceTypes == true
                    if (coerceTypes) {
                        if (field.isPresent()) {
                            final Object value = parseFieldForType(subStartElement, fieldName, field.get().getDataType(), recordValues);
                            if (value != null) {
                                recordValues.put(fieldName, value);
                            }
                        } else {
                            final Object value = parseUnknownField(subStartElement);
                            if (value != null) {
                                putUnknownTypeInMap(recordValues, fieldName, value);
                            }
                        }

                    } else {
                        final Object value = parseUnknownField(subStartElement);
                        if (value != null) {
                            putUnknownTypeInMap(recordValues, fieldName, value);
                        }
                    }
                }
            } else if (xmlEvent.isEndElement()) {
                break;
            } else if (xmlEvent.isCharacters()) {
                final Characters characters = xmlEvent.asCharacters();
                if (!characters.isWhiteSpace()) {
                    if (contentFieldName != null) {
                        final Optional<RecordField> field = schema.getField(contentFieldName);
                        if (field.isPresent()) {
                            Object value = parseCharacterForType(characters, contentFieldName, field.get().getDataType());
                            recordValues.put(contentFieldName, value);
                        }
                    } else {
                        logger.debug("Found content for field that is defined as record but property \"Field Name for Content\" is not set. " +
                                "The content will not be added to record.");
                    }
                }
            }
        }
        for (final Map.Entry<String,Object> entry : recordValues.entrySet()) {
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

    private void putUnknownTypeInMap(Map<String, Object> values, String fieldName, Object fieldValue) {
        final Object oldValues = values.get(fieldName);

        if (oldValues != null) {
            if (oldValues instanceof List) {
                ((List) oldValues).add(fieldValue);
            } else {
                values.put(fieldName, new ArrayList<Object>(){{ add(oldValues); add(fieldValue); }});
            }
        } else {
            values.put(fieldName, fieldValue);
        }
    }

    private Object parseAttributeForType(Attribute attribute, String fieldName, DataType dataType) {
        switch (dataType.getFieldType()) {
            case BOOLEAN:
            case BYTE:
            case CHAR:
            case DOUBLE:
            case FLOAT:
            case INT:
            case LONG:
            case SHORT:
            case STRING:
            case DATE:
            case TIME:
            case TIMESTAMP: {
                return DataTypeUtils.convertType(attribute.getValue(), dataType, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT, fieldName);
            }
        }
        return null;
    }

    private Object parseCharacterForType(Characters character, String fieldName, DataType dataType) {
        switch (dataType.getFieldType()) {
            case BOOLEAN:
            case BYTE:
            case CHAR:
            case DOUBLE:
            case FLOAT:
            case INT:
            case LONG:
            case SHORT:
            case STRING:
            case DATE:
            case TIME:
            case TIMESTAMP: {
                return DataTypeUtils.convertType(character.toString(), dataType, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT, fieldName);
            }
        }
        return null;
    }

    private void skipElement() throws XMLStreamException {
        while(xmlEventReader.hasNext()){
            final XMLEvent xmlEvent = xmlEventReader.nextEvent();

            if (xmlEvent.isStartElement()) {
                skipElement();
            }
            if (xmlEvent.isEndElement()) {
                return;
            }
        }
    }

    @Override
    public RecordSchema getSchema() {
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
}
