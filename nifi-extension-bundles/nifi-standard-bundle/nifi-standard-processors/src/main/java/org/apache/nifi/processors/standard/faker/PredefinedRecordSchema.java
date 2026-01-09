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
package org.apache.nifi.processors.standard.faker;

import net.datafaker.Faker;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Predefined record schemas for the GenerateRecord processor.
 * These schemas provide ready-to-use templates for generating fake data
 * without requiring manual configuration of dynamic properties.
 */
public enum PredefinedRecordSchema implements DescribedValue {

    PERSON("Person", "A person with name, contact information, and address") {
        @Override
        public RecordSchema getSchema(boolean nullable) {
            List<RecordField> addressFields = Arrays.asList(
                    new RecordField("street", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("city", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("state", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("zipCode", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("country", RecordFieldType.STRING.getDataType(), nullable)
            );
            RecordSchema addressSchema = new SimpleRecordSchema(addressFields);

            List<RecordField> fields = Arrays.asList(
                    new RecordField("id", RecordFieldType.UUID.getDataType(), nullable),
                    new RecordField("firstName", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("lastName", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("email", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("phoneNumber", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("dateOfBirth", RecordFieldType.DATE.getDataType(), nullable),
                    new RecordField("age", RecordFieldType.INT.getDataType(), nullable),
                    new RecordField("active", RecordFieldType.BOOLEAN.getDataType(), nullable),
                    new RecordField("address", RecordFieldType.RECORD.getRecordDataType(addressSchema), nullable)
            );
            return new SimpleRecordSchema(fields);
        }

        @Override
        public Map<String, Object> generateValues(Faker faker, RecordSchema schema, int nullPercentage) {
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("id", generateNullableValue(nullPercentage, faker, f -> UUID.randomUUID()));
            values.put("firstName", generateNullableValue(nullPercentage, faker, f -> f.name().firstName()));
            values.put("lastName", generateNullableValue(nullPercentage, faker, f -> f.name().lastName()));
            values.put("email", generateNullableValue(nullPercentage, faker, f -> f.internet().emailAddress()));
            values.put("phoneNumber", generateNullableValue(nullPercentage, faker, f -> f.phoneNumber().phoneNumber()));
            values.put("dateOfBirth", generateNullableValue(nullPercentage, faker, f -> {
                LocalDate birthday = f.timeAndDate().birthday(18, 80);
                return Date.valueOf(birthday);
            }));
            values.put("age", generateNullableValue(nullPercentage, faker, f -> f.number().numberBetween(18, 80)));
            values.put("active", generateNullableValue(nullPercentage, faker, f -> f.bool().bool()));

            // Generate nested address record
            Map<String, Object> addressValues = new LinkedHashMap<>();
            addressValues.put("street", generateNullableValue(nullPercentage, faker, f -> f.address().streetAddress()));
            addressValues.put("city", generateNullableValue(nullPercentage, faker, f -> f.address().city()));
            addressValues.put("state", generateNullableValue(nullPercentage, faker, f -> f.address().state()));
            addressValues.put("zipCode", generateNullableValue(nullPercentage, faker, f -> f.address().zipCode()));
            addressValues.put("country", generateNullableValue(nullPercentage, faker, f -> f.address().country()));

            RecordSchema addressSchema = schema.getField("address").get().getDataType().getFieldType() == RecordFieldType.RECORD
                    ? ((RecordDataType) schema.getField("address").get().getDataType()).getChildSchema()
                    : null;

            if (addressSchema != null) {
                values.put("address", generateNullableValue(nullPercentage, faker, f -> new MapRecord(addressSchema, addressValues)));
            }

            return values;
        }
    },

    ORDER("Order", "An e-commerce order with line items, amounts, and timestamps") {
        @Override
        public RecordSchema getSchema(boolean nullable) {
            List<RecordField> lineItemFields = Arrays.asList(
                    new RecordField("productId", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("productName", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("quantity", RecordFieldType.INT.getDataType(), nullable),
                    new RecordField("unitPrice", RecordFieldType.DOUBLE.getDataType(), nullable)
            );
            RecordSchema lineItemSchema = new SimpleRecordSchema(lineItemFields);

            List<RecordField> fields = Arrays.asList(
                    new RecordField("orderId", RecordFieldType.UUID.getDataType(), nullable),
                    new RecordField("customerId", RecordFieldType.UUID.getDataType(), nullable),
                    new RecordField("customerName", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("customerEmail", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("orderDate", RecordFieldType.DATE.getDataType(), nullable),
                    new RecordField("orderTime", RecordFieldType.TIME.getDataType(), nullable),
                    new RecordField("orderTimestamp", RecordFieldType.TIMESTAMP.getDataType(), nullable),
                    new RecordField("totalAmount", RecordFieldType.DECIMAL.getDecimalDataType(10, 2), nullable),
                    new RecordField("currency", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("status", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("shipped", RecordFieldType.BOOLEAN.getDataType(), nullable),
                    new RecordField("itemCount", RecordFieldType.INT.getDataType(), nullable),
                    new RecordField("lineItems", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(lineItemSchema)), nullable)
            );
            return new SimpleRecordSchema(fields);
        }

        @Override
        public Map<String, Object> generateValues(Faker faker, RecordSchema schema, int nullPercentage) {
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("orderId", generateNullableValue(nullPercentage, faker, f -> UUID.randomUUID()));
            values.put("customerId", generateNullableValue(nullPercentage, faker, f -> UUID.randomUUID()));
            values.put("customerName", generateNullableValue(nullPercentage, faker, f -> f.name().fullName()));
            values.put("customerEmail", generateNullableValue(nullPercentage, faker, f -> f.internet().emailAddress()));

            Instant orderInstant = faker.timeAndDate().past(365, TimeUnit.DAYS);
            values.put("orderDate", generateNullableValue(nullPercentage, faker, f -> new Date(orderInstant.toEpochMilli())));
            values.put("orderTime", generateNullableValue(nullPercentage, faker, f -> new Time(orderInstant.toEpochMilli())));
            values.put("orderTimestamp", generateNullableValue(nullPercentage, faker, f -> new Timestamp(orderInstant.toEpochMilli())));

            String[] statuses = {"PENDING", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"};
            String status = statuses[faker.number().numberBetween(0, statuses.length)];
            values.put("status", generateNullableValue(nullPercentage, faker, f -> status));
            values.put("shipped", generateNullableValue(nullPercentage, faker, f -> "SHIPPED".equals(status) || "DELIVERED".equals(status)));

            String[] currencies = {"USD", "EUR", "GBP", "JPY", "CAD"};
            values.put("currency", generateNullableValue(nullPercentage, faker, f -> currencies[f.number().numberBetween(0, currencies.length)]));

            // Generate line items
            int itemCount = faker.number().numberBetween(1, 5);
            values.put("itemCount", generateNullableValue(nullPercentage, faker, f -> itemCount));

            RecordSchema lineItemSchema = null;
            if (schema.getField("lineItems").get().getDataType().getFieldType() == RecordFieldType.ARRAY) {
                DataType elementType = ((ArrayDataType) schema.getField("lineItems").get().getDataType()).getElementType();
                if (elementType.getFieldType() == RecordFieldType.RECORD) {
                    lineItemSchema = ((RecordDataType) elementType).getChildSchema();
                }
            }

            double totalAmount = 0.0;
            Object[] lineItems = new Object[itemCount];
            for (int i = 0; i < itemCount; i++) {
                Map<String, Object> lineItemValues = new LinkedHashMap<>();
                lineItemValues.put("productId", "PRD-" + faker.number().digits(8));
                lineItemValues.put("productName", faker.commerce().productName());
                int quantity = faker.number().numberBetween(1, 10);
                double unitPrice = faker.number().randomDouble(2, 10, 500);
                lineItemValues.put("quantity", quantity);
                lineItemValues.put("unitPrice", unitPrice);
                totalAmount += quantity * unitPrice;

                if (lineItemSchema != null) {
                    lineItems[i] = new MapRecord(lineItemSchema, lineItemValues);
                }
            }
            values.put("lineItems", generateNullableValue(nullPercentage, faker, f -> lineItems));
            final double finalTotal = totalAmount;
            values.put("totalAmount", generateNullableValue(nullPercentage, faker, f -> BigDecimal.valueOf(finalTotal).setScale(2, RoundingMode.HALF_UP)));

            return values;
        }
    },

    EVENT("Event", "A timestamped event with metadata and tags") {
        @Override
        public RecordSchema getSchema(boolean nullable) {
            List<RecordField> fields = Arrays.asList(
                    new RecordField("eventId", RecordFieldType.UUID.getDataType(), nullable),
                    new RecordField("eventType", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("eventDate", RecordFieldType.DATE.getDataType(), nullable),
                    new RecordField("eventTime", RecordFieldType.TIME.getDataType(), nullable),
                    new RecordField("eventTimestamp", RecordFieldType.TIMESTAMP.getDataType(), nullable),
                    new RecordField("source", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("severity", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("message", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("processed", RecordFieldType.BOOLEAN.getDataType(), nullable),
                    new RecordField("retryCount", RecordFieldType.INT.getDataType(), nullable),
                    new RecordField("durationMs", RecordFieldType.LONG.getDataType(), nullable),
                    new RecordField("tags", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType()), nullable),
                    new RecordField("metadata", RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType()), nullable)
            );
            return new SimpleRecordSchema(fields);
        }

        @Override
        public Map<String, Object> generateValues(Faker faker, RecordSchema schema, int nullPercentage) {
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("eventId", generateNullableValue(nullPercentage, faker, f -> UUID.randomUUID()));

            String[] eventTypes = {"USER_LOGIN", "USER_LOGOUT", "PURCHASE", "PAGE_VIEW", "API_CALL", "ERROR", "WARNING", "AUDIT"};
            values.put("eventType", generateNullableValue(nullPercentage, faker, f -> eventTypes[f.number().numberBetween(0, eventTypes.length)]));

            Instant eventInstant = faker.timeAndDate().past(30, TimeUnit.DAYS);
            values.put("eventDate", generateNullableValue(nullPercentage, faker, f -> new Date(eventInstant.toEpochMilli())));
            values.put("eventTime", generateNullableValue(nullPercentage, faker, f -> new Time(eventInstant.toEpochMilli())));
            values.put("eventTimestamp", generateNullableValue(nullPercentage, faker, f -> new Timestamp(eventInstant.toEpochMilli())));

            String[] sources = {"web-app", "mobile-app", "api-gateway", "batch-processor", "stream-processor"};
            values.put("source", generateNullableValue(nullPercentage, faker, f -> sources[f.number().numberBetween(0, sources.length)]));

            String[] severities = {"DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"};
            values.put("severity", generateNullableValue(nullPercentage, faker, f -> severities[f.number().numberBetween(0, severities.length)]));
            values.put("message", generateNullableValue(nullPercentage, faker, f -> f.lorem().sentence()));
            values.put("processed", generateNullableValue(nullPercentage, faker, f -> f.bool().bool()));
            values.put("retryCount", generateNullableValue(nullPercentage, faker, f -> f.number().numberBetween(0, 5)));
            values.put("durationMs", generateNullableValue(nullPercentage, faker, f -> f.number().numberBetween(1L, 10000L)));

            // Generate tags array
            String[] possibleTags = {"important", "urgent", "reviewed", "automated", "manual", "verified", "pending"};
            int tagCount = faker.number().numberBetween(1, 4);
            String[] tags = new String[tagCount];
            for (int i = 0; i < tagCount; i++) {
                tags[i] = possibleTags[faker.number().numberBetween(0, possibleTags.length)];
            }
            values.put("tags", generateNullableValue(nullPercentage, faker, f -> tags));

            // Generate metadata map
            Map<String, String> metadata = new HashMap<>();
            metadata.put("version", "1." + faker.number().numberBetween(0, 10));
            metadata.put("environment", faker.options().option("dev", "staging", "prod"));
            metadata.put("region", faker.options().option("us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"));
            metadata.put("correlationId", UUID.randomUUID().toString());
            values.put("metadata", generateNullableValue(nullPercentage, faker, f -> metadata));

            return values;
        }
    },

    SENSOR("Sensor", "An IoT sensor reading with location and measurements") {
        @Override
        public RecordSchema getSchema(boolean nullable) {
            List<RecordField> locationFields = Arrays.asList(
                    new RecordField("latitude", RecordFieldType.DOUBLE.getDataType(), nullable),
                    new RecordField("longitude", RecordFieldType.DOUBLE.getDataType(), nullable),
                    new RecordField("altitude", RecordFieldType.DOUBLE.getDataType(), nullable)
            );
            RecordSchema locationSchema = new SimpleRecordSchema(locationFields);

            List<RecordField> fields = Arrays.asList(
                    new RecordField("sensorId", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("deviceType", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("manufacturer", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("readingTimestamp", RecordFieldType.TIMESTAMP.getDataType(), nullable),
                    new RecordField("temperature", RecordFieldType.DOUBLE.getDataType(), nullable),
                    new RecordField("humidity", RecordFieldType.DOUBLE.getDataType(), nullable),
                    new RecordField("pressure", RecordFieldType.DOUBLE.getDataType(), nullable),
                    new RecordField("batteryLevel", RecordFieldType.INT.getDataType(), nullable),
                    new RecordField("signalStrength", RecordFieldType.INT.getDataType(), nullable),
                    new RecordField("online", RecordFieldType.BOOLEAN.getDataType(), nullable),
                    new RecordField("location", RecordFieldType.RECORD.getRecordDataType(locationSchema), nullable)
            );
            return new SimpleRecordSchema(fields);
        }

        @Override
        public Map<String, Object> generateValues(Faker faker, RecordSchema schema, int nullPercentage) {
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("sensorId", generateNullableValue(nullPercentage, faker, f -> "SNS-" + f.number().digits(10)));

            String[] deviceTypes = {"TEMPERATURE", "HUMIDITY", "PRESSURE", "MOTION", "LIGHT", "AIR_QUALITY", "MULTI"};
            values.put("deviceType", generateNullableValue(nullPercentage, faker, f -> deviceTypes[f.number().numberBetween(0, deviceTypes.length)]));

            String[] manufacturers = {"SensorCorp", "IoTech", "SmartDevices", "DataSense", "EnviroMonitor"};
            values.put("manufacturer", generateNullableValue(nullPercentage, faker, f -> manufacturers[f.number().numberBetween(0, manufacturers.length)]));

            values.put("readingTimestamp", generateNullableValue(nullPercentage, faker, f -> new Timestamp(System.currentTimeMillis() - f.number().numberBetween(0, 3600000))));
            values.put("temperature", generateNullableValue(nullPercentage, faker, f -> f.number().randomDouble(2, -20, 45)));
            values.put("humidity", generateNullableValue(nullPercentage, faker, f -> f.number().randomDouble(2, 0, 100)));
            values.put("pressure", generateNullableValue(nullPercentage, faker, f -> f.number().randomDouble(2, 980, 1050)));
            values.put("batteryLevel", generateNullableValue(nullPercentage, faker, f -> f.number().numberBetween(0, 100)));
            values.put("signalStrength", generateNullableValue(nullPercentage, faker, f -> f.number().numberBetween(-100, -30)));
            values.put("online", generateNullableValue(nullPercentage, faker, f -> f.bool().bool()));

            // Generate nested location record
            Map<String, Object> locationValues = new LinkedHashMap<>();
            locationValues.put("latitude", faker.number().randomDouble(6, -90, 90));
            locationValues.put("longitude", faker.number().randomDouble(6, -180, 180));
            locationValues.put("altitude", faker.number().randomDouble(2, 0, 3000));

            RecordSchema locationSchema = schema.getField("location").get().getDataType().getFieldType() == RecordFieldType.RECORD
                    ? ((RecordDataType) schema.getField("location").get().getDataType()).getChildSchema()
                    : null;

            if (locationSchema != null) {
                values.put("location", generateNullableValue(nullPercentage, faker, f -> new MapRecord(locationSchema, locationValues)));
            }

            return values;
        }
    },

    PRODUCT("Product", "A product catalog entry with pricing and inventory") {
        @Override
        public RecordSchema getSchema(boolean nullable) {
            List<RecordField> dimensionFields = Arrays.asList(
                    new RecordField("length", RecordFieldType.DOUBLE.getDataType(), nullable),
                    new RecordField("width", RecordFieldType.DOUBLE.getDataType(), nullable),
                    new RecordField("height", RecordFieldType.DOUBLE.getDataType(), nullable),
                    new RecordField("weight", RecordFieldType.DOUBLE.getDataType(), nullable)
            );
            RecordSchema dimensionSchema = new SimpleRecordSchema(dimensionFields);

            List<RecordField> fields = Arrays.asList(
                    new RecordField("productId", RecordFieldType.UUID.getDataType(), nullable),
                    new RecordField("sku", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("name", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("description", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("category", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("brand", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("price", RecordFieldType.DECIMAL.getDecimalDataType(10, 2), nullable),
                    new RecordField("currency", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("inStock", RecordFieldType.BOOLEAN.getDataType(), nullable),
                    new RecordField("quantity", RecordFieldType.INT.getDataType(), nullable),
                    new RecordField("rating", RecordFieldType.DOUBLE.getDataType(), nullable),
                    new RecordField("reviewCount", RecordFieldType.INT.getDataType(), nullable),
                    new RecordField("createdDate", RecordFieldType.DATE.getDataType(), nullable),
                    new RecordField("lastUpdated", RecordFieldType.TIMESTAMP.getDataType(), nullable),
                    new RecordField("tags", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType()), nullable),
                    new RecordField("dimensions", RecordFieldType.RECORD.getRecordDataType(dimensionSchema), nullable)
            );
            return new SimpleRecordSchema(fields);
        }

        @Override
        public Map<String, Object> generateValues(Faker faker, RecordSchema schema, int nullPercentage) {
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("productId", generateNullableValue(nullPercentage, faker, f -> UUID.randomUUID()));
            values.put("sku", generateNullableValue(nullPercentage, faker, f -> "SKU-" + f.number().digits(8)));
            values.put("name", generateNullableValue(nullPercentage, faker, f -> f.commerce().productName()));
            values.put("description", generateNullableValue(nullPercentage, faker, f -> f.lorem().paragraph()));
            values.put("category", generateNullableValue(nullPercentage, faker, f -> f.commerce().department()));
            values.put("brand", generateNullableValue(nullPercentage, faker, f -> f.company().name()));
            values.put("price", generateNullableValue(nullPercentage, faker, f -> BigDecimal.valueOf(f.number().randomDouble(2, 5, 2000)).setScale(2, RoundingMode.HALF_UP)));

            String[] currencies = {"USD", "EUR", "GBP"};
            values.put("currency", generateNullableValue(nullPercentage, faker, f -> currencies[f.number().numberBetween(0, currencies.length)]));

            int quantity = faker.number().numberBetween(0, 500);
            values.put("inStock", generateNullableValue(nullPercentage, faker, f -> quantity > 0));
            values.put("quantity", generateNullableValue(nullPercentage, faker, f -> quantity));
            values.put("rating", generateNullableValue(nullPercentage, faker, f -> f.number().randomDouble(1, 1, 5)));
            values.put("reviewCount", generateNullableValue(nullPercentage, faker, f -> f.number().numberBetween(0, 5000)));
            values.put("createdDate", generateNullableValue(nullPercentage, faker, f -> new Date(f.timeAndDate().past(365, TimeUnit.DAYS).toEpochMilli())));
            values.put("lastUpdated", generateNullableValue(nullPercentage, faker, f -> new Timestamp(f.timeAndDate().past(30, TimeUnit.DAYS).toEpochMilli())));

            // Generate tags array
            String[] possibleTags = {"new", "sale", "bestseller", "limited", "exclusive", "eco-friendly", "premium"};
            int tagCount = faker.number().numberBetween(0, 4);
            String[] tags = new String[tagCount];
            for (int i = 0; i < tagCount; i++) {
                tags[i] = possibleTags[faker.number().numberBetween(0, possibleTags.length)];
            }
            values.put("tags", generateNullableValue(nullPercentage, faker, f -> tags));

            // Generate nested dimensions record
            Map<String, Object> dimensionValues = new LinkedHashMap<>();
            dimensionValues.put("length", faker.number().randomDouble(2, 1, 100));
            dimensionValues.put("width", faker.number().randomDouble(2, 1, 100));
            dimensionValues.put("height", faker.number().randomDouble(2, 1, 100));
            dimensionValues.put("weight", faker.number().randomDouble(2, 1, 50));

            RecordSchema dimensionSchema = schema.getField("dimensions").get().getDataType().getFieldType() == RecordFieldType.RECORD
                    ? ((RecordDataType) schema.getField("dimensions").get().getDataType()).getChildSchema()
                    : null;

            if (dimensionSchema != null) {
                values.put("dimensions", generateNullableValue(nullPercentage, faker, f -> new MapRecord(dimensionSchema, dimensionValues)));
            }

            return values;
        }
    },

    STOCK_TRADE("Stock Trade", "A stock market trade with pricing and volume") {
        @Override
        public RecordSchema getSchema(boolean nullable) {
            List<RecordField> fields = Arrays.asList(
                    new RecordField("tradeId", RecordFieldType.UUID.getDataType(), nullable),
                    new RecordField("symbol", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("companyName", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("exchange", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("tradeType", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("tradeTimestamp", RecordFieldType.TIMESTAMP.getDataType(), nullable),
                    new RecordField("price", RecordFieldType.DECIMAL.getDecimalDataType(12, 4), nullable),
                    new RecordField("quantity", RecordFieldType.LONG.getDataType(), nullable),
                    new RecordField("totalValue", RecordFieldType.DECIMAL.getDecimalDataType(16, 2), nullable),
                    new RecordField("currency", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("bidPrice", RecordFieldType.DECIMAL.getDecimalDataType(12, 4), nullable),
                    new RecordField("askPrice", RecordFieldType.DECIMAL.getDecimalDataType(12, 4), nullable),
                    new RecordField("high52Week", RecordFieldType.DECIMAL.getDecimalDataType(12, 4), nullable),
                    new RecordField("low52Week", RecordFieldType.DECIMAL.getDecimalDataType(12, 4), nullable),
                    new RecordField("marketCap", RecordFieldType.LONG.getDataType(), nullable),
                    new RecordField("settled", RecordFieldType.BOOLEAN.getDataType(), nullable)
            );
            return new SimpleRecordSchema(fields);
        }

        @Override
        public Map<String, Object> generateValues(Faker faker, RecordSchema schema, int nullPercentage) {
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("tradeId", generateNullableValue(nullPercentage, faker, f -> UUID.randomUUID()));

            String[] symbols = {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "TSLA", "NVDA", "JPM", "V", "JNJ"};
            String[] companies = {"Apple Inc.", "Alphabet Inc.", "Microsoft Corp.", "Amazon.com Inc.", "Meta Platforms Inc.",
                    "Tesla Inc.", "NVIDIA Corp.", "JPMorgan Chase & Co.", "Visa Inc.", "Johnson & Johnson"};
            int symbolIdx = faker.number().numberBetween(0, symbols.length);
            values.put("symbol", generateNullableValue(nullPercentage, faker, f -> symbols[symbolIdx]));
            values.put("companyName", generateNullableValue(nullPercentage, faker, f -> companies[symbolIdx]));

            String[] exchanges = {"NYSE", "NASDAQ", "LSE", "TSE"};
            values.put("exchange", generateNullableValue(nullPercentage, faker, f -> exchanges[f.number().numberBetween(0, exchanges.length)]));

            String[] tradeTypes = {"BUY", "SELL"};
            values.put("tradeType", generateNullableValue(nullPercentage, faker, f -> tradeTypes[f.number().numberBetween(0, tradeTypes.length)]));
            values.put("tradeTimestamp", generateNullableValue(nullPercentage, faker, f -> new Timestamp(System.currentTimeMillis() - f.number().numberBetween(0, 86400000))));

            double price = faker.number().randomDouble(4, 10, 3000);
            long quantity = faker.number().numberBetween(1, 10000);
            values.put("price", generateNullableValue(nullPercentage, faker, f -> BigDecimal.valueOf(price).setScale(4, RoundingMode.HALF_UP)));
            values.put("quantity", generateNullableValue(nullPercentage, faker, f -> quantity));
            values.put("totalValue", generateNullableValue(nullPercentage, faker, f -> BigDecimal.valueOf(price * quantity).setScale(2, RoundingMode.HALF_UP)));
            values.put("currency", generateNullableValue(nullPercentage, faker, f -> "USD"));
            values.put("bidPrice", generateNullableValue(nullPercentage, faker, f -> BigDecimal.valueOf(price * 0.999).setScale(4, RoundingMode.HALF_UP)));
            values.put("askPrice", generateNullableValue(nullPercentage, faker, f -> BigDecimal.valueOf(price * 1.001).setScale(4, RoundingMode.HALF_UP)));
            values.put("high52Week", generateNullableValue(nullPercentage, faker, f -> BigDecimal.valueOf(price * 1.5).setScale(4, RoundingMode.HALF_UP)));
            values.put("low52Week", generateNullableValue(nullPercentage, faker, f -> BigDecimal.valueOf(price * 0.6).setScale(4, RoundingMode.HALF_UP)));
            values.put("marketCap", generateNullableValue(nullPercentage, faker, f -> f.number().numberBetween(1_000_000_000L, 3_000_000_000_000L)));
            values.put("settled", generateNullableValue(nullPercentage, faker, f -> f.bool().bool()));

            return values;
        }
    },

    COMPLETE_EXAMPLE("Complete Example", "A comprehensive schema demonstrating all supported data types including nested records, arrays, and maps") {
        @Override
        public RecordSchema getSchema(boolean nullable) {
            // Deepest nested record: Coordinates
            List<RecordField> coordinateFields = Arrays.asList(
                    new RecordField("latitude", RecordFieldType.DOUBLE.getDataType(), nullable),
                    new RecordField("longitude", RecordFieldType.DOUBLE.getDataType(), nullable)
            );
            RecordSchema coordinateSchema = new SimpleRecordSchema(coordinateFields);

            // Address record with nested coordinates
            List<RecordField> addressFields = Arrays.asList(
                    new RecordField("street", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("city", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("state", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("zipCode", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("country", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("coordinates", RecordFieldType.RECORD.getRecordDataType(coordinateSchema), nullable)
            );
            RecordSchema addressSchema = new SimpleRecordSchema(addressFields);

            // Profile record with nested address
            List<RecordField> profileFields = Arrays.asList(
                    new RecordField("firstName", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("lastName", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("email", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("age", RecordFieldType.INT.getDataType(), nullable),
                    new RecordField("verified", RecordFieldType.BOOLEAN.getDataType(), nullable),
                    new RecordField("address", RecordFieldType.RECORD.getRecordDataType(addressSchema), nullable)
            );
            RecordSchema profileSchema = new SimpleRecordSchema(profileFields);

            // Order record for array of records
            List<RecordField> orderFields = Arrays.asList(
                    new RecordField("orderId", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("amount", RecordFieldType.DOUBLE.getDataType(), nullable),
                    new RecordField("currency", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("placed", RecordFieldType.DATE.getDataType(), nullable),
                    new RecordField("shipped", RecordFieldType.BOOLEAN.getDataType(), nullable)
            );
            RecordSchema orderSchema = new SimpleRecordSchema(orderFields);

            // Main schema with all types
            List<RecordField> fields = Arrays.asList(
                    // Basic types
                    new RecordField("id", RecordFieldType.UUID.getDataType(), nullable),
                    new RecordField("active", RecordFieldType.BOOLEAN.getDataType(), nullable),
                    new RecordField("score", RecordFieldType.INT.getDataType(), nullable),
                    new RecordField("count", RecordFieldType.LONG.getDataType(), nullable),
                    new RecordField("rating", RecordFieldType.DOUBLE.getDataType(), nullable),
                    new RecordField("price", RecordFieldType.FLOAT.getDataType(), nullable),
                    new RecordField("balance", RecordFieldType.DECIMAL.getDecimalDataType(12, 2), nullable),
                    new RecordField("initial", RecordFieldType.CHAR.getDataType(), nullable),
                    new RecordField("flags", RecordFieldType.BYTE.getDataType(), nullable),
                    new RecordField("rank", RecordFieldType.SHORT.getDataType(), nullable),

                    // Date/Time types
                    new RecordField("createdDate", RecordFieldType.DATE.getDataType(), nullable),
                    new RecordField("lastLoginTime", RecordFieldType.TIME.getDataType(), nullable),
                    new RecordField("lastModified", RecordFieldType.TIMESTAMP.getDataType(), nullable),

                    // Complex types
                    new RecordField("tags", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType()), nullable),
                    new RecordField("scores", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.INT.getDataType()), nullable),
                    new RecordField("metadata", RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType()), nullable),
                    new RecordField("profile", RecordFieldType.RECORD.getRecordDataType(profileSchema), nullable),
                    new RecordField("orders", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(orderSchema)), nullable)
            );
            return new SimpleRecordSchema(fields);
        }

        @Override
        public Map<String, Object> generateValues(Faker faker, RecordSchema schema, int nullPercentage) {
            Map<String, Object> values = new LinkedHashMap<>();

            // Basic types
            values.put("id", generateNullableValue(nullPercentage, faker, f -> UUID.randomUUID()));
            values.put("active", generateNullableValue(nullPercentage, faker, f -> f.bool().bool()));
            values.put("score", generateNullableValue(nullPercentage, faker, f -> f.number().numberBetween(0, 100)));
            values.put("count", generateNullableValue(nullPercentage, faker, f -> f.number().numberBetween(0L, 1_000_000L)));
            values.put("rating", generateNullableValue(nullPercentage, faker, f -> f.number().randomDouble(2, 0, 5)));
            values.put("price", generateNullableValue(nullPercentage, faker, f -> (float) f.number().randomDouble(2, 1, 1000)));
            values.put("balance", generateNullableValue(nullPercentage, faker, f -> BigDecimal.valueOf(f.number().randomDouble(2, -10000, 50000)).setScale(2, RoundingMode.HALF_UP)));
            values.put("initial", generateNullableValue(nullPercentage, faker, f -> (char) ('A' + f.number().numberBetween(0, 26))));
            values.put("flags", generateNullableValue(nullPercentage, faker, f -> (byte) f.number().numberBetween(0, 127)));
            values.put("rank", generateNullableValue(nullPercentage, faker, f -> (short) f.number().numberBetween(1, 1000)));

            // Date/Time types
            Instant pastInstant = faker.timeAndDate().past(365, TimeUnit.DAYS);
            values.put("createdDate", generateNullableValue(nullPercentage, faker, f -> new Date(pastInstant.toEpochMilli())));
            values.put("lastLoginTime", generateNullableValue(nullPercentage, faker, f -> new Time(f.timeAndDate().past(1, TimeUnit.DAYS).toEpochMilli())));
            values.put("lastModified", generateNullableValue(nullPercentage, faker, f -> new Timestamp(f.timeAndDate().past(7, TimeUnit.DAYS).toEpochMilli())));

            // Array of strings
            String[] possibleTags = {"important", "urgent", "reviewed", "automated", "verified", "pending", "approved"};
            int tagCount = faker.number().numberBetween(1, 5);
            String[] tags = new String[tagCount];
            for (int i = 0; i < tagCount; i++) {
                tags[i] = possibleTags[faker.number().numberBetween(0, possibleTags.length)];
            }
            values.put("tags", generateNullableValue(nullPercentage, faker, f -> tags));

            // Array of integers
            int scoreCount = faker.number().numberBetween(3, 8);
            Integer[] scores = new Integer[scoreCount];
            for (int i = 0; i < scoreCount; i++) {
                scores[i] = faker.number().numberBetween(50, 100);
            }
            values.put("scores", generateNullableValue(nullPercentage, faker, f -> scores));

            // Map
            Map<String, String> metadata = new HashMap<>();
            metadata.put("source", faker.options().option("web", "mobile", "api", "batch"));
            metadata.put("version", "1." + faker.number().numberBetween(0, 10));
            metadata.put("environment", faker.options().option("dev", "staging", "prod"));
            metadata.put("region", faker.options().option("us-east-1", "us-west-2", "eu-west-1"));
            values.put("metadata", generateNullableValue(nullPercentage, faker, f -> metadata));

            // Nested profile record (3 levels deep: profile -> address -> coordinates)
            RecordSchema profileSchema = schema.getField("profile").get().getDataType().getFieldType() == RecordFieldType.RECORD
                    ? ((RecordDataType) schema.getField("profile").get().getDataType()).getChildSchema()
                    : null;

            if (profileSchema != null) {
                Map<String, Object> profileValues = new LinkedHashMap<>();
                profileValues.put("firstName", faker.name().firstName());
                profileValues.put("lastName", faker.name().lastName());
                profileValues.put("email", faker.internet().emailAddress());
                profileValues.put("age", faker.number().numberBetween(18, 80));
                profileValues.put("verified", faker.bool().bool());

                RecordSchema addressSchema = profileSchema.getField("address").get().getDataType().getFieldType() == RecordFieldType.RECORD
                        ? ((RecordDataType) profileSchema.getField("address").get().getDataType()).getChildSchema()
                        : null;

                if (addressSchema != null) {
                    Map<String, Object> addressValues = new LinkedHashMap<>();
                    addressValues.put("street", faker.address().streetAddress());
                    addressValues.put("city", faker.address().city());
                    addressValues.put("state", faker.address().state());
                    addressValues.put("zipCode", faker.address().zipCode());
                    addressValues.put("country", faker.address().country());

                    RecordSchema coordinateSchema = addressSchema.getField("coordinates").get().getDataType().getFieldType() == RecordFieldType.RECORD
                            ? ((RecordDataType) addressSchema.getField("coordinates").get().getDataType()).getChildSchema()
                            : null;

                    if (coordinateSchema != null) {
                        Map<String, Object> coordValues = new LinkedHashMap<>();
                        coordValues.put("latitude", faker.number().randomDouble(6, -90, 90));
                        coordValues.put("longitude", faker.number().randomDouble(6, -180, 180));
                        addressValues.put("coordinates", new MapRecord(coordinateSchema, coordValues));
                    }

                    profileValues.put("address", new MapRecord(addressSchema, addressValues));
                }

                values.put("profile", generateNullableValue(nullPercentage, faker, f -> new MapRecord(profileSchema, profileValues)));
            }

            // Array of order records
            RecordSchema orderSchema = null;
            if (schema.getField("orders").get().getDataType().getFieldType() == RecordFieldType.ARRAY) {
                DataType elementType = ((ArrayDataType) schema.getField("orders").get().getDataType()).getElementType();
                if (elementType.getFieldType() == RecordFieldType.RECORD) {
                    orderSchema = ((RecordDataType) elementType).getChildSchema();
                }
            }

            if (orderSchema != null) {
                final RecordSchema finalOrderSchema = orderSchema;
                int orderCount = faker.number().numberBetween(1, 4);
                Object[] orders = new Object[orderCount];
                for (int i = 0; i < orderCount; i++) {
                    Map<String, Object> orderValues = new LinkedHashMap<>();
                    orderValues.put("orderId", "ORD-" + faker.number().digits(8));
                    orderValues.put("amount", faker.number().randomDouble(2, 10, 500));
                    String[] currencies = {"USD", "EUR", "GBP"};
                    orderValues.put("currency", currencies[faker.number().numberBetween(0, currencies.length)]);
                    orderValues.put("placed", new Date(faker.timeAndDate().past(90, TimeUnit.DAYS).toEpochMilli()));
                    orderValues.put("shipped", faker.bool().bool());
                    orders[i] = new MapRecord(finalOrderSchema, orderValues);
                }
                values.put("orders", generateNullableValue(nullPercentage, faker, f -> orders));
            }

            return values;
        }
    };

    private final String displayName;
    private final String description;

    PredefinedRecordSchema(String displayName, String description) {
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getValue() {
        return name();
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }

    /**
     * Get the record schema for this predefined schema type.
     *
     * @param nullable whether fields should be nullable
     * @return the RecordSchema
     */
    public abstract RecordSchema getSchema(boolean nullable);

    /**
     * Generate random values for all fields in this schema.
     *
     * @param faker the Faker instance to use for generating random data
     * @param schema the RecordSchema to generate values for
     * @param nullPercentage the percentage chance (0-100) that nullable fields will be null
     * @return a Map of field names to generated values
     */
    public abstract Map<String, Object> generateValues(Faker faker, RecordSchema schema, int nullPercentage);

    /**
     * Generate a record with random values.
     *
     * @param faker the Faker instance to use for generating random data
     * @param nullable whether fields should be nullable
     * @param nullPercentage the percentage chance (0-100) that nullable fields will be null
     * @return a Record with generated values
     */
    public Record generateRecord(Faker faker, boolean nullable, int nullPercentage) {
        RecordSchema schema = getSchema(nullable);
        Map<String, Object> values = generateValues(faker, schema, nullPercentage);
        return new MapRecord(schema, values);
    }

    /**
     * Helper method to generate a value with a chance of being null.
     */
    protected static <T> T generateNullableValue(int nullPercentage, Faker faker, Function<Faker, T> generator) {
        if (nullPercentage > 0 && faker.number().numberBetween(0, 100) < nullPercentage) {
            return null;
        }
        return generator.apply(faker);
    }

    /**
     * Get a predefined schema by name, or null if not found or empty.
     *
     * @param name the name of the predefined schema
     * @return the PredefinedRecordSchema, or null if not found or name is null/empty
     */
    public static PredefinedRecordSchema fromName(String name) {
        if (name == null || name.isEmpty()) {
            return null;
        }
        try {
            return valueOf(name);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
