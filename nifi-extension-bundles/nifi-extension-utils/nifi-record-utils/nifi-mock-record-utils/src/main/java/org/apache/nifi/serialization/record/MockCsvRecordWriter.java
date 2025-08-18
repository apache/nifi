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
package org.apache.nifi.serialization.record;

import org.apache.nifi.serialization.RecordSetWriterFactory;

import java.util.Map;
import java.util.function.Function;

/**
 * {@code MockCsvRecordWriter} is a concrete implementation of a {@link RecordSetWriterFactory}
 * that produces CSV-formatted output. It extends the {@link MockRecordWriter} and provides a
 * fluent {@link Builder} to configure properties such as header inclusion, value quoting,
 * output buffering, and schema specification.
 * <p>
 * This class is suitable for use in testing scenarios or simple serialization tasks where
 * CSV output is needed and full schema compliance is not required.
 *
 * <p><strong>Example usage:</strong></p>
 * <pre>{@code
 * RecordSchema schema = ...; // define schema as needed
 * MockCsvRecordWriter writer = MockCsvRecordWriter.builder()
 *     .withHeader("id,name,age")
 *     .quoteValues(true)
 *     .bufferOutput(true)
 *     .withSchema(schema)
 *     .withSeparator(attributes -> attributes.getOrDefault("csv.separator", ","))
 *     .build();
 * }</pre>
 */
public final class MockCsvRecordWriter extends MockRecordWriter {

    /**
     * Returns a new {@link Builder} instance for constructing a {@code MockCsvRecordWriter}.
     *
     * @return builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Constructs a MockCsvRecordWriter with the specified configuration.
     *
     * @param header optional CSV header line (e.g., "id,name,age"), may be {@code null}
     * @param quoteValues whether to quote individual field values (useful for text fields)
     * @param failAfterN if positive, causes writer to throw IOException after writing N records (used in testing)
     * @param bufferOutput whether to wrap output stream in a {@link java.io.BufferedOutputStream}
     * @param schema optional {@link RecordSchema} used for field name and type resolution
     * @param mapToSeparator a function to dynamically resolve the field separator based on FlowFile attributes
     */
    private MockCsvRecordWriter(
            String header,
            boolean quoteValues,
            int failAfterN,
            boolean bufferOutput,
            RecordSchema schema,
            Function<Map<String, String>, String> mapToSeparator) {
        super(header, quoteValues, failAfterN, bufferOutput, schema, mapToSeparator);
    }

    /**
     * Builder class for fluent construction of {@link MockCsvRecordWriter} instances.
     */
    public static class Builder {
        private String header = null;
        private boolean quoteValues = true;
        private int failAfterN = -1;
        private boolean bufferOutput = false;
        private RecordSchema schema = null;
        private Function<Map<String, String>, String> mapToSeparator = attributes -> DEFAULT_SEPARATOR;

        private Builder() {
        }

        /**
         * Specifies a header line to be written before the CSV data.
         *
         * @param header the header line (e.g., "id,name,age"); can be {@code null} to omit header
         * @return the current {@code Builder} instance
         */
        public Builder withHeader(String header) {
            this.header = header;
            return this;
        }

        /**
         * Specifies whether field values should be enclosed in double quotes.
         *
         * @param quoteValues {@code true} to quote values, {@code false} otherwise
         * @return the current {@code Builder} instance
         */
        public Builder quoteValues(boolean quoteValues) {
            this.quoteValues = quoteValues;
            return this;
        }

        /**
         * Configures the writer to throw an {@link java.io.IOException} after writing a given number of records.
         * Primarily used for testing failure scenarios.
         *
         * @param failAfterN the number of records after which to fail; use -1 to disable
         * @return the current {@code Builder} instance
         */
        public Builder failAfterN(int failAfterN) {
            this.failAfterN = failAfterN;
            return this;
        }

        /**
         * Specifies whether the output stream should be wrapped in a {@link java.io.BufferedOutputStream}.
         *
         * @param bufferOutput {@code true} to enable buffering, {@code false} to write directly
         * @return the current {@code Builder} instance
         */
        public Builder bufferOutput(boolean bufferOutput) {
            this.bufferOutput = bufferOutput;
            return this;
        }

        /**
         * Sets the schema to be used for writing records.
         *
         * @param schema the {@link RecordSchema}; can be {@code null} if schema inference is acceptable
         * @return the current {@code Builder} instance
         */
        public Builder withSchema(RecordSchema schema) {
            this.schema = schema;
            return this;
        }

        /**
         * Specifies a function to dynamically resolve the field separator based on FlowFile attributes.
         * <p><strong>Example usage:</strong></p>
         * <pre>{@code
         *     builder.withSeparator(attributes -> attributes.getOrDefault("csv.separator", ","));
         * }</pre>
         *
         * @param mapToSeparator a function that maps FlowFile attributes to a separator string
         * @return the current {@code Builder} instance
         */
        public Builder withSeparator(Function<Map<String, String>, String> mapToSeparator) {
            this.mapToSeparator = mapToSeparator;
            return this;
        }


        /**
         * Constructs a new {@link MockCsvRecordWriter} instance using the configured parameters.
         *
         * @return a fully initialized {@link MockCsvRecordWriter}
         */
        public MockCsvRecordWriter build() {
            return new MockCsvRecordWriter(header, quoteValues, failAfterN, bufferOutput, schema, mapToSeparator);
        }
    }
}
