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

    PERSON("Person", "A person with name, contact information, and address (schema.org/Person)") {
        @Override
        public RecordSchema getSchema(boolean nullable) {
            // PostalAddress fields per schema.org/PostalAddress
            List<RecordField> addressFields = Arrays.asList(
                    new RecordField("streetAddress", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("addressLocality", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("addressRegion", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("postalCode", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("addressCountry", RecordFieldType.STRING.getDataType(), nullable)
            );
            RecordSchema addressSchema = new SimpleRecordSchema(addressFields);

            // Person fields per schema.org/Person
            List<RecordField> fields = Arrays.asList(
                    new RecordField("identifier", RecordFieldType.UUID.getDataType(), nullable),
                    new RecordField("givenName", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("familyName", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("email", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("telephone", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("birthDate", RecordFieldType.DATE.getDataType(), nullable),
                    new RecordField("age", RecordFieldType.INT.getDataType(), nullable),
                    new RecordField("active", RecordFieldType.BOOLEAN.getDataType(), nullable),
                    new RecordField("address", RecordFieldType.RECORD.getRecordDataType(addressSchema), nullable)
            );
            return new SimpleRecordSchema(fields);
        }

        @Override
        public Map<String, Object> generateValues(Faker faker, RecordSchema schema, int nullPercentage) {
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("identifier", generateNullableValue(nullPercentage, faker, f -> UUID.randomUUID()));
            values.put("givenName", generateNullableValue(nullPercentage, faker, f -> f.name().firstName()));
            values.put("familyName", generateNullableValue(nullPercentage, faker, f -> f.name().lastName()));
            values.put("email", generateNullableValue(nullPercentage, faker, f -> f.internet().emailAddress()));
            values.put("telephone", generateNullableValue(nullPercentage, faker, f -> f.phoneNumber().phoneNumber()));
            values.put("birthDate", generateNullableValue(nullPercentage, faker, f -> {
                LocalDate birthday = f.timeAndDate().birthday(18, 80);
                return Date.valueOf(birthday);
            }));
            values.put("age", generateNullableValue(nullPercentage, faker, f -> f.number().numberBetween(18, 80)));
            values.put("active", generateNullableValue(nullPercentage, faker, f -> f.bool().bool()));

            // Generate nested address record (PostalAddress)
            Map<String, Object> addressValues = new LinkedHashMap<>();
            addressValues.put("streetAddress", generateNullableValue(nullPercentage, faker, f -> f.address().streetAddress()));
            addressValues.put("addressLocality", generateNullableValue(nullPercentage, faker, f -> f.address().city()));
            addressValues.put("addressRegion", generateNullableValue(nullPercentage, faker, f -> f.address().state()));
            addressValues.put("postalCode", generateNullableValue(nullPercentage, faker, f -> f.address().zipCode()));
            addressValues.put("addressCountry", generateNullableValue(nullPercentage, faker, f -> f.address().country()));

            RecordSchema addressSchema = schema.getField("address").get().getDataType().getFieldType() == RecordFieldType.RECORD
                    ? ((RecordDataType) schema.getField("address").get().getDataType()).getChildSchema()
                    : null;

            if (addressSchema != null) {
                values.put("address", generateNullableValue(nullPercentage, faker, f -> new MapRecord(addressSchema, addressValues)));
            }

            return values;
        }
    },

    ORDER("Order", "An e-commerce order with line items, amounts, and timestamps (schema.org/Order)") {
        @Override
        public RecordSchema getSchema(boolean nullable) {
            // OrderItem fields per schema.org/OrderItem
            List<RecordField> orderedItemFields = Arrays.asList(
                    new RecordField("identifier", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("name", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("orderQuantity", RecordFieldType.INT.getDataType(), nullable),
                    new RecordField("price", RecordFieldType.DOUBLE.getDataType(), nullable)
            );
            RecordSchema orderedItemSchema = new SimpleRecordSchema(orderedItemFields);

            // Order fields per schema.org/Order
            List<RecordField> fields = Arrays.asList(
                    new RecordField("orderNumber", RecordFieldType.UUID.getDataType(), nullable),
                    new RecordField("customer", RecordFieldType.UUID.getDataType(), nullable),
                    new RecordField("customerName", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("customerEmail", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("orderDate", RecordFieldType.DATE.getDataType(), nullable),
                    new RecordField("orderTime", RecordFieldType.TIME.getDataType(), nullable),
                    new RecordField("orderDelivery", RecordFieldType.TIMESTAMP.getDataType(), nullable),
                    new RecordField("totalPrice", RecordFieldType.DECIMAL.getDecimalDataType(10, 2), nullable),
                    new RecordField("priceCurrency", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("orderStatus", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("isGift", RecordFieldType.BOOLEAN.getDataType(), nullable),
                    new RecordField("itemCount", RecordFieldType.INT.getDataType(), nullable),
                    new RecordField("orderedItem", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(orderedItemSchema)), nullable)
            );
            return new SimpleRecordSchema(fields);
        }

        @Override
        public Map<String, Object> generateValues(Faker faker, RecordSchema schema, int nullPercentage) {
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("orderNumber", generateNullableValue(nullPercentage, faker, f -> UUID.randomUUID()));
            values.put("customer", generateNullableValue(nullPercentage, faker, f -> UUID.randomUUID()));
            values.put("customerName", generateNullableValue(nullPercentage, faker, f -> f.name().fullName()));
            values.put("customerEmail", generateNullableValue(nullPercentage, faker, f -> f.internet().emailAddress()));

            Instant orderInstant = faker.timeAndDate().past(365, TimeUnit.DAYS);
            values.put("orderDate", generateNullableValue(nullPercentage, faker, f -> new Date(orderInstant.toEpochMilli())));
            values.put("orderTime", generateNullableValue(nullPercentage, faker, f -> new Time(orderInstant.toEpochMilli())));
            values.put("orderDelivery", generateNullableValue(nullPercentage, faker, f -> new Timestamp(orderInstant.toEpochMilli())));

            // OrderStatus values per schema.org/OrderStatus
            String[] statuses = {"OrderCancelled", "OrderDelivered", "OrderInTransit", "OrderPaymentDue",
                    "OrderPickupAvailable", "OrderProblem", "OrderProcessing", "OrderReturned"};
            String status = statuses[faker.number().numberBetween(0, statuses.length)];
            values.put("orderStatus", generateNullableValue(nullPercentage, faker, f -> status));
            values.put("isGift", generateNullableValue(nullPercentage, faker, f -> f.bool().bool()));

            // Use Faker's money provider for currency codes
            values.put("priceCurrency", generateNullableValue(nullPercentage, faker, f -> f.money().currencyCode()));

            // Generate ordered items
            int itemCount = faker.number().numberBetween(1, 5);
            values.put("itemCount", generateNullableValue(nullPercentage, faker, f -> itemCount));

            RecordSchema orderedItemSchema = null;
            if (schema.getField("orderedItem").get().getDataType().getFieldType() == RecordFieldType.ARRAY) {
                DataType elementType = ((ArrayDataType) schema.getField("orderedItem").get().getDataType()).getElementType();
                if (elementType.getFieldType() == RecordFieldType.RECORD) {
                    orderedItemSchema = ((RecordDataType) elementType).getChildSchema();
                }
            }

            double totalPrice = 0.0;
            Object[] orderedItems = new Object[itemCount];
            for (int i = 0; i < itemCount; i++) {
                Map<String, Object> orderedItemValues = new LinkedHashMap<>();
                orderedItemValues.put("identifier", "PRD-" + faker.number().digits(8));
                orderedItemValues.put("name", faker.commerce().productName());
                int quantity = faker.number().numberBetween(1, 10);
                double price = faker.number().randomDouble(2, 10, 500);
                orderedItemValues.put("orderQuantity", quantity);
                orderedItemValues.put("price", price);
                totalPrice += quantity * price;

                if (orderedItemSchema != null) {
                    orderedItems[i] = new MapRecord(orderedItemSchema, orderedItemValues);
                }
            }
            values.put("orderedItem", generateNullableValue(nullPercentage, faker, f -> orderedItems));
            final double finalTotal = totalPrice;
            values.put("totalPrice", generateNullableValue(nullPercentage, faker, f -> BigDecimal.valueOf(finalTotal).setScale(2, RoundingMode.HALF_UP)));

            return values;
        }
    },

    EVENT("Event", "A timestamped event with metadata and keywords (schema.org/Event)") {
        @Override
        public RecordSchema getSchema(boolean nullable) {
            // Event fields per schema.org/Event
            List<RecordField> fields = Arrays.asList(
                    new RecordField("identifier", RecordFieldType.UUID.getDataType(), nullable),
                    new RecordField("additionalType", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("startDate", RecordFieldType.DATE.getDataType(), nullable),
                    new RecordField("startTime", RecordFieldType.TIME.getDataType(), nullable),
                    new RecordField("endDate", RecordFieldType.TIMESTAMP.getDataType(), nullable),
                    new RecordField("organizer", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("eventStatus", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("description", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("isAccessibleForFree", RecordFieldType.BOOLEAN.getDataType(), nullable),
                    new RecordField("attendeeCount", RecordFieldType.INT.getDataType(), nullable),
                    new RecordField("duration", RecordFieldType.LONG.getDataType(), nullable),
                    new RecordField("keywords", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType()), nullable),
                    new RecordField("additionalProperty", RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType()), nullable)
            );
            return new SimpleRecordSchema(fields);
        }

        @Override
        public Map<String, Object> generateValues(Faker faker, RecordSchema schema, int nullPercentage) {
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("identifier", generateNullableValue(nullPercentage, faker, f -> UUID.randomUUID()));

            // Use Faker's hacker provider for event type naming
            values.put("additionalType", generateNullableValue(nullPercentage, faker, f ->
                    f.hacker().verb().toUpperCase() + "_" + f.hacker().noun().toUpperCase()));

            Instant eventInstant = faker.timeAndDate().past(30, TimeUnit.DAYS);
            values.put("startDate", generateNullableValue(nullPercentage, faker, f -> new Date(eventInstant.toEpochMilli())));
            values.put("startTime", generateNullableValue(nullPercentage, faker, f -> new Time(eventInstant.toEpochMilli())));
            values.put("endDate", generateNullableValue(nullPercentage, faker, f -> new Timestamp(eventInstant.toEpochMilli())));

            // Use Faker's app provider for organizer names
            values.put("organizer", generateNullableValue(nullPercentage, faker, f -> f.app().name().toLowerCase().replace(" ", "-")));

            // EventStatus values per schema.org/EventStatusType
            String[] statuses = {"EventCancelled", "EventMovedOnline", "EventPostponed", "EventRescheduled", "EventScheduled"};
            values.put("eventStatus", generateNullableValue(nullPercentage, faker, f -> statuses[f.number().numberBetween(0, statuses.length)]));
            values.put("description", generateNullableValue(nullPercentage, faker, f -> f.lorem().sentence()));
            values.put("isAccessibleForFree", generateNullableValue(nullPercentage, faker, f -> f.bool().bool()));
            values.put("attendeeCount", generateNullableValue(nullPercentage, faker, f -> f.number().numberBetween(0, 5)));
            values.put("duration", generateNullableValue(nullPercentage, faker, f -> f.number().numberBetween(1L, 10000L)));

            // Generate keywords array using Faker's marketing buzzwords
            int keywordCount = faker.number().numberBetween(1, 4);
            String[] keywords = new String[keywordCount];
            for (int i = 0; i < keywordCount; i++) {
                keywords[i] = faker.marketing().buzzwords().toLowerCase();
            }
            values.put("keywords", generateNullableValue(nullPercentage, faker, f -> keywords));

            // Generate additionalProperty map using Faker providers
            Map<String, String> additionalProperty = new HashMap<>();
            additionalProperty.put("version", faker.app().version());
            additionalProperty.put("environment", faker.options().option("dev", "staging", "prod"));
            // Use Faker's AWS provider for region
            additionalProperty.put("region", faker.aws().region());
            additionalProperty.put("correlationId", UUID.randomUUID().toString());
            values.put("additionalProperty", generateNullableValue(nullPercentage, faker, f -> additionalProperty));

            return values;
        }
    },

    SENSOR("Sensor", "An IoT sensor reading with geo coordinates and measurements") {
        @Override
        public RecordSchema getSchema(boolean nullable) {
            // GeoCoordinates fields per schema.org/GeoCoordinates
            List<RecordField> geoFields = Arrays.asList(
                    new RecordField("latitude", RecordFieldType.DOUBLE.getDataType(), nullable),
                    new RecordField("longitude", RecordFieldType.DOUBLE.getDataType(), nullable),
                    new RecordField("elevation", RecordFieldType.DOUBLE.getDataType(), nullable)
            );
            RecordSchema geoSchema = new SimpleRecordSchema(geoFields);

            List<RecordField> fields = Arrays.asList(
                    new RecordField("identifier", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("additionalType", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("manufacturer", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("dateCreated", RecordFieldType.TIMESTAMP.getDataType(), nullable),
                    new RecordField("temperature", RecordFieldType.DOUBLE.getDataType(), nullable),
                    new RecordField("humidity", RecordFieldType.DOUBLE.getDataType(), nullable),
                    new RecordField("pressure", RecordFieldType.DOUBLE.getDataType(), nullable),
                    new RecordField("batteryLevel", RecordFieldType.INT.getDataType(), nullable),
                    new RecordField("signalStrength", RecordFieldType.INT.getDataType(), nullable),
                    new RecordField("isActive", RecordFieldType.BOOLEAN.getDataType(), nullable),
                    new RecordField("geo", RecordFieldType.RECORD.getRecordDataType(geoSchema), nullable)
            );
            return new SimpleRecordSchema(fields);
        }

        @Override
        public Map<String, Object> generateValues(Faker faker, RecordSchema schema, int nullPercentage) {
            Map<String, Object> values = new LinkedHashMap<>();
            // Use Faker's device provider for serial number
            values.put("identifier", generateNullableValue(nullPercentage, faker, f -> f.device().serial()));

            // Use Faker's device provider for platform/type
            values.put("additionalType", generateNullableValue(nullPercentage, faker, f -> f.device().platform()));

            // Use Faker's device provider for manufacturer
            values.put("manufacturer", generateNullableValue(nullPercentage, faker, f -> f.device().manufacturer()));

            values.put("dateCreated", generateNullableValue(nullPercentage, faker, f -> new Timestamp(System.currentTimeMillis() - f.number().numberBetween(0, 3600000))));
            values.put("temperature", generateNullableValue(nullPercentage, faker, f -> f.number().randomDouble(2, -20, 45)));
            values.put("humidity", generateNullableValue(nullPercentage, faker, f -> f.number().randomDouble(2, 0, 100)));
            values.put("pressure", generateNullableValue(nullPercentage, faker, f -> f.number().randomDouble(2, 980, 1050)));
            values.put("batteryLevel", generateNullableValue(nullPercentage, faker, f -> f.number().numberBetween(0, 100)));
            values.put("signalStrength", generateNullableValue(nullPercentage, faker, f -> f.number().numberBetween(-100, -30)));
            values.put("isActive", generateNullableValue(nullPercentage, faker, f -> f.bool().bool()));

            // Generate nested geo record (GeoCoordinates)
            Map<String, Object> geoValues = new LinkedHashMap<>();
            geoValues.put("latitude", faker.number().randomDouble(6, -90, 90));
            geoValues.put("longitude", faker.number().randomDouble(6, -180, 180));
            geoValues.put("elevation", faker.number().randomDouble(2, 0, 3000));

            RecordSchema geoSchema = schema.getField("geo").get().getDataType().getFieldType() == RecordFieldType.RECORD
                    ? ((RecordDataType) schema.getField("geo").get().getDataType()).getChildSchema()
                    : null;

            if (geoSchema != null) {
                values.put("geo", generateNullableValue(nullPercentage, faker, f -> new MapRecord(geoSchema, geoValues)));
            }

            return values;
        }
    },

    PRODUCT("Product", "A product catalog entry with pricing and inventory (schema.org/Product)") {
        @Override
        public RecordSchema getSchema(boolean nullable) {
            // QuantitativeValue for dimensions per schema.org/QuantitativeValue
            List<RecordField> dimensionFields = Arrays.asList(
                    new RecordField("depth", RecordFieldType.DOUBLE.getDataType(), nullable),
                    new RecordField("width", RecordFieldType.DOUBLE.getDataType(), nullable),
                    new RecordField("height", RecordFieldType.DOUBLE.getDataType(), nullable),
                    new RecordField("weight", RecordFieldType.DOUBLE.getDataType(), nullable)
            );
            RecordSchema dimensionSchema = new SimpleRecordSchema(dimensionFields);

            // Product fields per schema.org/Product
            List<RecordField> fields = Arrays.asList(
                    new RecordField("identifier", RecordFieldType.UUID.getDataType(), nullable),
                    new RecordField("sku", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("name", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("description", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("category", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("brand", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("price", RecordFieldType.DECIMAL.getDecimalDataType(10, 2), nullable),
                    new RecordField("priceCurrency", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("availability", RecordFieldType.BOOLEAN.getDataType(), nullable),
                    new RecordField("inventoryLevel", RecordFieldType.INT.getDataType(), nullable),
                    new RecordField("ratingValue", RecordFieldType.DOUBLE.getDataType(), nullable),
                    new RecordField("reviewCount", RecordFieldType.INT.getDataType(), nullable),
                    new RecordField("dateCreated", RecordFieldType.DATE.getDataType(), nullable),
                    new RecordField("dateModified", RecordFieldType.TIMESTAMP.getDataType(), nullable),
                    new RecordField("keywords", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType()), nullable),
                    new RecordField("additionalProperty", RecordFieldType.RECORD.getRecordDataType(dimensionSchema), nullable)
            );
            return new SimpleRecordSchema(fields);
        }

        @Override
        public Map<String, Object> generateValues(Faker faker, RecordSchema schema, int nullPercentage) {
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("identifier", generateNullableValue(nullPercentage, faker, f -> UUID.randomUUID()));
            values.put("sku", generateNullableValue(nullPercentage, faker, f -> "SKU-" + f.number().digits(8)));
            values.put("name", generateNullableValue(nullPercentage, faker, f -> f.commerce().productName()));
            values.put("description", generateNullableValue(nullPercentage, faker, f -> f.lorem().paragraph()));
            values.put("category", generateNullableValue(nullPercentage, faker, f -> f.commerce().department()));
            // Use Faker's commerce provider for brand
            values.put("brand", generateNullableValue(nullPercentage, faker, f -> f.commerce().brand()));
            values.put("price", generateNullableValue(nullPercentage, faker, f -> BigDecimal.valueOf(f.number().randomDouble(2, 5, 2000)).setScale(2, RoundingMode.HALF_UP)));

            // Use Faker's money provider for currency codes
            values.put("priceCurrency", generateNullableValue(nullPercentage, faker, f -> f.money().currencyCode()));

            int inventoryLevel = faker.number().numberBetween(0, 500);
            values.put("availability", generateNullableValue(nullPercentage, faker, f -> inventoryLevel > 0));
            values.put("inventoryLevel", generateNullableValue(nullPercentage, faker, f -> inventoryLevel));
            values.put("ratingValue", generateNullableValue(nullPercentage, faker, f -> f.number().randomDouble(1, 1, 5)));
            values.put("reviewCount", generateNullableValue(nullPercentage, faker, f -> f.number().numberBetween(0, 5000)));
            values.put("dateCreated", generateNullableValue(nullPercentage, faker, f -> new Date(f.timeAndDate().past(365, TimeUnit.DAYS).toEpochMilli())));
            values.put("dateModified", generateNullableValue(nullPercentage, faker, f -> new Timestamp(f.timeAndDate().past(30, TimeUnit.DAYS).toEpochMilli())));

            // Generate keywords array using Faker's marketing buzzwords
            int keywordCount = faker.number().numberBetween(0, 4);
            String[] keywords = new String[keywordCount];
            for (int i = 0; i < keywordCount; i++) {
                keywords[i] = faker.marketing().buzzwords().toLowerCase();
            }
            values.put("keywords", generateNullableValue(nullPercentage, faker, f -> keywords));

            // Generate nested additionalProperty record (dimensions)
            Map<String, Object> dimensionValues = new LinkedHashMap<>();
            dimensionValues.put("depth", faker.number().randomDouble(2, 1, 100));
            dimensionValues.put("width", faker.number().randomDouble(2, 1, 100));
            dimensionValues.put("height", faker.number().randomDouble(2, 1, 100));
            dimensionValues.put("weight", faker.number().randomDouble(2, 1, 50));

            RecordSchema dimensionSchema = schema.getField("additionalProperty").get().getDataType().getFieldType() == RecordFieldType.RECORD
                    ? ((RecordDataType) schema.getField("additionalProperty").get().getDataType()).getChildSchema()
                    : null;

            if (dimensionSchema != null) {
                values.put("additionalProperty", generateNullableValue(nullPercentage, faker, f -> new MapRecord(dimensionSchema, dimensionValues)));
            }

            return values;
        }
    },

    STOCK_TRADE("Stock Trade", "A stock market trade with pricing and volume") {
        @Override
        public RecordSchema getSchema(boolean nullable) {
            // Using schema.org naming conventions where applicable
            List<RecordField> fields = Arrays.asList(
                    new RecordField("identifier", RecordFieldType.UUID.getDataType(), nullable),
                    new RecordField("tickerSymbol", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("name", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("exchange", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("actionType", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("dateCreated", RecordFieldType.TIMESTAMP.getDataType(), nullable),
                    new RecordField("price", RecordFieldType.DECIMAL.getDecimalDataType(12, 4), nullable),
                    new RecordField("orderQuantity", RecordFieldType.LONG.getDataType(), nullable),
                    new RecordField("totalPrice", RecordFieldType.DECIMAL.getDecimalDataType(16, 2), nullable),
                    new RecordField("priceCurrency", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("bidPrice", RecordFieldType.DECIMAL.getDecimalDataType(12, 4), nullable),
                    new RecordField("askPrice", RecordFieldType.DECIMAL.getDecimalDataType(12, 4), nullable),
                    new RecordField("highPrice", RecordFieldType.DECIMAL.getDecimalDataType(12, 4), nullable),
                    new RecordField("lowPrice", RecordFieldType.DECIMAL.getDecimalDataType(12, 4), nullable),
                    new RecordField("marketCap", RecordFieldType.LONG.getDataType(), nullable),
                    new RecordField("isSettled", RecordFieldType.BOOLEAN.getDataType(), nullable)
            );
            return new SimpleRecordSchema(fields);
        }

        @Override
        public Map<String, Object> generateValues(Faker faker, RecordSchema schema, int nullPercentage) {
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("identifier", generateNullableValue(nullPercentage, faker, f -> UUID.randomUUID()));

            // Use Faker's stock provider for symbols (randomly choose between NASDAQ and NYSE)
            values.put("tickerSymbol", generateNullableValue(nullPercentage, faker, f ->
                    f.bool().bool() ? f.stock().nsdqSymbol() : f.stock().nyseSymbol()));
            // Use Faker's company provider for company names
            values.put("name", generateNullableValue(nullPercentage, faker, f -> f.company().name()));

            // Use Faker's stock provider for exchanges
            values.put("exchange", generateNullableValue(nullPercentage, faker, f -> f.stock().exchanges()));

            // Trade types are fundamental financial terms (BUY/SELL)
            String[] actionTypes = {"BuyAction", "SellAction"};
            values.put("actionType", generateNullableValue(nullPercentage, faker, f -> actionTypes[f.number().numberBetween(0, actionTypes.length)]));
            values.put("dateCreated", generateNullableValue(nullPercentage, faker, f -> new Timestamp(System.currentTimeMillis() - f.number().numberBetween(0, 86400000))));

            double price = faker.number().randomDouble(4, 10, 3000);
            long orderQuantity = faker.number().numberBetween(1, 10000);
            values.put("price", generateNullableValue(nullPercentage, faker, f -> BigDecimal.valueOf(price).setScale(4, RoundingMode.HALF_UP)));
            values.put("orderQuantity", generateNullableValue(nullPercentage, faker, f -> orderQuantity));
            values.put("totalPrice", generateNullableValue(nullPercentage, faker, f -> BigDecimal.valueOf(price * orderQuantity).setScale(2, RoundingMode.HALF_UP)));
            // Use Faker's money provider for currency codes
            values.put("priceCurrency", generateNullableValue(nullPercentage, faker, f -> f.money().currencyCode()));
            values.put("bidPrice", generateNullableValue(nullPercentage, faker, f -> BigDecimal.valueOf(price * 0.999).setScale(4, RoundingMode.HALF_UP)));
            values.put("askPrice", generateNullableValue(nullPercentage, faker, f -> BigDecimal.valueOf(price * 1.001).setScale(4, RoundingMode.HALF_UP)));
            values.put("highPrice", generateNullableValue(nullPercentage, faker, f -> BigDecimal.valueOf(price * 1.5).setScale(4, RoundingMode.HALF_UP)));
            values.put("lowPrice", generateNullableValue(nullPercentage, faker, f -> BigDecimal.valueOf(price * 0.6).setScale(4, RoundingMode.HALF_UP)));
            values.put("marketCap", generateNullableValue(nullPercentage, faker, f -> f.number().numberBetween(1_000_000_000L, 3_000_000_000_000L)));
            values.put("isSettled", generateNullableValue(nullPercentage, faker, f -> f.bool().bool()));

            return values;
        }
    },

    COMPLETE_EXAMPLE("Complete Example", "A comprehensive schema demonstrating all supported data types including nested records, arrays, and maps") {
        @Override
        public RecordSchema getSchema(boolean nullable) {
            // GeoCoordinates per schema.org/GeoCoordinates
            List<RecordField> geoFields = Arrays.asList(
                    new RecordField("latitude", RecordFieldType.DOUBLE.getDataType(), nullable),
                    new RecordField("longitude", RecordFieldType.DOUBLE.getDataType(), nullable)
            );
            RecordSchema geoSchema = new SimpleRecordSchema(geoFields);

            // PostalAddress per schema.org/PostalAddress with nested geo
            List<RecordField> addressFields = Arrays.asList(
                    new RecordField("streetAddress", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("addressLocality", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("addressRegion", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("postalCode", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("addressCountry", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("geo", RecordFieldType.RECORD.getRecordDataType(geoSchema), nullable)
            );
            RecordSchema addressSchema = new SimpleRecordSchema(addressFields);

            // Person per schema.org/Person with nested address
            List<RecordField> personFields = Arrays.asList(
                    new RecordField("givenName", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("familyName", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("email", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("age", RecordFieldType.INT.getDataType(), nullable),
                    new RecordField("verified", RecordFieldType.BOOLEAN.getDataType(), nullable),
                    new RecordField("address", RecordFieldType.RECORD.getRecordDataType(addressSchema), nullable)
            );
            RecordSchema personSchema = new SimpleRecordSchema(personFields);

            // Order per schema.org/Order for array of records
            List<RecordField> orderFields = Arrays.asList(
                    new RecordField("orderNumber", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("totalPrice", RecordFieldType.DOUBLE.getDataType(), nullable),
                    new RecordField("priceCurrency", RecordFieldType.STRING.getDataType(), nullable),
                    new RecordField("orderDate", RecordFieldType.DATE.getDataType(), nullable),
                    new RecordField("isGift", RecordFieldType.BOOLEAN.getDataType(), nullable)
            );
            RecordSchema orderSchema = new SimpleRecordSchema(orderFields);

            // Main schema with all types using schema.org naming
            List<RecordField> fields = Arrays.asList(
                    // Basic types
                    new RecordField("identifier", RecordFieldType.UUID.getDataType(), nullable),
                    new RecordField("isActive", RecordFieldType.BOOLEAN.getDataType(), nullable),
                    new RecordField("score", RecordFieldType.INT.getDataType(), nullable),
                    new RecordField("count", RecordFieldType.LONG.getDataType(), nullable),
                    new RecordField("ratingValue", RecordFieldType.DOUBLE.getDataType(), nullable),
                    new RecordField("price", RecordFieldType.FLOAT.getDataType(), nullable),
                    new RecordField("balance", RecordFieldType.DECIMAL.getDecimalDataType(12, 2), nullable),
                    new RecordField("initial", RecordFieldType.CHAR.getDataType(), nullable),
                    new RecordField("flags", RecordFieldType.BYTE.getDataType(), nullable),
                    new RecordField("position", RecordFieldType.SHORT.getDataType(), nullable),

                    // Date/Time types per schema.org
                    new RecordField("dateCreated", RecordFieldType.DATE.getDataType(), nullable),
                    new RecordField("lastLogin", RecordFieldType.TIME.getDataType(), nullable),
                    new RecordField("dateModified", RecordFieldType.TIMESTAMP.getDataType(), nullable),

                    // Complex types
                    new RecordField("keywords", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType()), nullable),
                    new RecordField("scores", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.INT.getDataType()), nullable),
                    new RecordField("additionalProperty", RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType()), nullable),
                    new RecordField("person", RecordFieldType.RECORD.getRecordDataType(personSchema), nullable),
                    new RecordField("orderedItem", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(orderSchema)), nullable)
            );
            return new SimpleRecordSchema(fields);
        }

        @Override
        public Map<String, Object> generateValues(Faker faker, RecordSchema schema, int nullPercentage) {
            Map<String, Object> values = new LinkedHashMap<>();

            // Basic types
            values.put("identifier", generateNullableValue(nullPercentage, faker, f -> UUID.randomUUID()));
            values.put("isActive", generateNullableValue(nullPercentage, faker, f -> f.bool().bool()));
            values.put("score", generateNullableValue(nullPercentage, faker, f -> f.number().numberBetween(0, 100)));
            values.put("count", generateNullableValue(nullPercentage, faker, f -> f.number().numberBetween(0L, 1_000_000L)));
            values.put("ratingValue", generateNullableValue(nullPercentage, faker, f -> f.number().randomDouble(2, 0, 5)));
            values.put("price", generateNullableValue(nullPercentage, faker, f -> (float) f.number().randomDouble(2, 1, 1000)));
            values.put("balance", generateNullableValue(nullPercentage, faker, f -> BigDecimal.valueOf(f.number().randomDouble(2, -10000, 50000)).setScale(2, RoundingMode.HALF_UP)));
            values.put("initial", generateNullableValue(nullPercentage, faker, f -> (char) ('A' + f.number().numberBetween(0, 26))));
            values.put("flags", generateNullableValue(nullPercentage, faker, f -> (byte) f.number().numberBetween(0, 127)));
            values.put("position", generateNullableValue(nullPercentage, faker, f -> (short) f.number().numberBetween(1, 1000)));

            // Date/Time types per schema.org
            Instant pastInstant = faker.timeAndDate().past(365, TimeUnit.DAYS);
            values.put("dateCreated", generateNullableValue(nullPercentage, faker, f -> new Date(pastInstant.toEpochMilli())));
            values.put("lastLogin", generateNullableValue(nullPercentage, faker, f -> new Time(f.timeAndDate().past(1, TimeUnit.DAYS).toEpochMilli())));
            values.put("dateModified", generateNullableValue(nullPercentage, faker, f -> new Timestamp(f.timeAndDate().past(7, TimeUnit.DAYS).toEpochMilli())));

            // Array of strings (keywords) using Faker's word provider
            int keywordCount = faker.number().numberBetween(1, 5);
            String[] keywords = new String[keywordCount];
            for (int i = 0; i < keywordCount; i++) {
                keywords[i] = faker.word().adjective();
            }
            values.put("keywords", generateNullableValue(nullPercentage, faker, f -> keywords));

            // Array of integers
            int scoreCount = faker.number().numberBetween(3, 8);
            Integer[] scores = new Integer[scoreCount];
            for (int i = 0; i < scoreCount; i++) {
                scores[i] = faker.number().numberBetween(50, 100);
            }
            values.put("scores", generateNullableValue(nullPercentage, faker, f -> scores));

            // Map (additionalProperty) using Faker providers
            Map<String, String> additionalProperty = new HashMap<>();
            additionalProperty.put("source", faker.app().name().toLowerCase().replace(" ", "-"));
            additionalProperty.put("version", faker.app().version());
            additionalProperty.put("environment", faker.options().option("dev", "staging", "prod"));
            // Use Faker's AWS provider for region
            additionalProperty.put("region", faker.aws().region());
            values.put("additionalProperty", generateNullableValue(nullPercentage, faker, f -> additionalProperty));

            // Nested person record (3 levels deep: person -> address -> geo)
            RecordSchema personSchema = schema.getField("person").get().getDataType().getFieldType() == RecordFieldType.RECORD
                    ? ((RecordDataType) schema.getField("person").get().getDataType()).getChildSchema()
                    : null;

            if (personSchema != null) {
                Map<String, Object> personValues = new LinkedHashMap<>();
                personValues.put("givenName", faker.name().firstName());
                personValues.put("familyName", faker.name().lastName());
                personValues.put("email", faker.internet().emailAddress());
                personValues.put("age", faker.number().numberBetween(18, 80));
                personValues.put("verified", faker.bool().bool());

                RecordSchema addressSchema = personSchema.getField("address").get().getDataType().getFieldType() == RecordFieldType.RECORD
                        ? ((RecordDataType) personSchema.getField("address").get().getDataType()).getChildSchema()
                        : null;

                if (addressSchema != null) {
                    Map<String, Object> addressValues = new LinkedHashMap<>();
                    addressValues.put("streetAddress", faker.address().streetAddress());
                    addressValues.put("addressLocality", faker.address().city());
                    addressValues.put("addressRegion", faker.address().state());
                    addressValues.put("postalCode", faker.address().zipCode());
                    addressValues.put("addressCountry", faker.address().country());

                    RecordSchema geoSchema = addressSchema.getField("geo").get().getDataType().getFieldType() == RecordFieldType.RECORD
                            ? ((RecordDataType) addressSchema.getField("geo").get().getDataType()).getChildSchema()
                            : null;

                    if (geoSchema != null) {
                        Map<String, Object> geoValues = new LinkedHashMap<>();
                        geoValues.put("latitude", faker.number().randomDouble(6, -90, 90));
                        geoValues.put("longitude", faker.number().randomDouble(6, -180, 180));
                        addressValues.put("geo", new MapRecord(geoSchema, geoValues));
                    }

                    personValues.put("address", new MapRecord(addressSchema, addressValues));
                }

                values.put("person", generateNullableValue(nullPercentage, faker, f -> new MapRecord(personSchema, personValues)));
            }

            // Array of order records (orderedItem)
            RecordSchema orderSchema = null;
            if (schema.getField("orderedItem").get().getDataType().getFieldType() == RecordFieldType.ARRAY) {
                DataType elementType = ((ArrayDataType) schema.getField("orderedItem").get().getDataType()).getElementType();
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
                    orderValues.put("orderNumber", "ORD-" + faker.number().digits(8));
                    orderValues.put("totalPrice", faker.number().randomDouble(2, 10, 500));
                    // Use Faker's money provider for currency codes
                    orderValues.put("priceCurrency", faker.money().currencyCode());
                    orderValues.put("orderDate", new Date(faker.timeAndDate().past(90, TimeUnit.DAYS).toEpochMilli()));
                    orderValues.put("isGift", faker.bool().bool());
                    orders[i] = new MapRecord(finalOrderSchema, orderValues);
                }
                values.put("orderedItem", generateNullableValue(nullPercentage, faker, f -> orders));
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
