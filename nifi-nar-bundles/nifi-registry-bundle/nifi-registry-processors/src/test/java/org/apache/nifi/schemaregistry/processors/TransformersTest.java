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
package org.apache.nifi.schemaregistry.processors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

@RunWith(JUnitParamsRunner.class)
public class TransformersTest {

    private final ClassLoader classLoader = getClass().getClassLoader();

    @Test
    public void validateCSVtoAvroPair() throws Exception {
        String data = "John Dow|13|blue";
        String fooSchemaText = "{\"namespace\": \"example.avro\", " + "\"type\": \"record\", " + "\"name\": \"User\", "
                + "\"fields\": [ " + "{\"name\": \"name\", \"type\": \"string\"}, "
                + "{\"name\": \"favorite_number\",  \"type\": \"int\"}, "
                + "{\"name\": \"favorite_color\", \"type\": \"string\"} " + "]" + "}";

        Schema schema = new Schema.Parser().parse(fooSchemaText);

        // CSV -> AVRO -> CSV
        ByteArrayInputStream in = new ByteArrayInputStream(data.getBytes());
        GenericRecord record = CSVUtils.read(in, '|', schema, '\"');
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        AvroUtils.write(record, out);
        byte[] avro = out.toByteArray();

        in = new ByteArrayInputStream(avro);
        record = AvroUtils.read(in, schema);
        out = new ByteArrayOutputStream();
        CSVUtils.write(record, '|', out);
        byte[] csv = out.toByteArray();
        assertEquals(data, new String(csv, StandardCharsets.UTF_8));
    }

    @Test
    public void validateCSVtoJsonPair() throws Exception {
        String data = "John Dow|13|blue";
        String fooSchemaText = "{\"namespace\": \"example.avro\", " + "\"type\": \"record\", " + "\"name\": \"User\", "
                + "\"fields\": [ " + "{\"name\": \"name\", \"type\": \"string\"}, "
                + "{\"name\": \"favorite_number\",  \"type\": \"int\"}, "
                + "{\"name\": \"favorite_color\", \"type\": \"string\"} " + "]" + "}";

        Schema schema = new Schema.Parser().parse(fooSchemaText);

        // CSV -> JSON -> CSV
        ByteArrayInputStream in = new ByteArrayInputStream(data.getBytes());
        GenericRecord record = CSVUtils.read(in, '|', schema, '\"');
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        JsonUtils.write(record, out);
        byte[] json = out.toByteArray();

        assertEquals("{\"name\":\"John Dow\",\"favorite_number\":13,\"favorite_color\":\"blue\"}", new String(json, StandardCharsets.UTF_8));

        in = new ByteArrayInputStream(json);
        record = JsonUtils.read(in, schema);
        out = new ByteArrayOutputStream();
        CSVUtils.write(record, '|', out);
        byte[] csv = out.toByteArray();
        assertEquals(data, new String(csv, StandardCharsets.UTF_8));
    }

    @Test
    public void validateJsonToAvroPair() throws Exception {
        String data = "{\"name\":\"John Dow\",\"favorite_number\":13,\"favorite_color\":\"blue\"}";
        String fooSchemaText = "{\"namespace\": \"example.avro\", " + "\"type\": \"record\", " + "\"name\": \"User\", "
                + "\"fields\": [ " + "{\"name\": \"name\", \"type\": \"string\"}, "
                + "{\"name\": \"favorite_number\",  \"type\": \"int\"}, "
                + "{\"name\": \"favorite_color\", \"type\": \"string\"} " + "]" + "}";

        Schema schema = new Schema.Parser().parse(fooSchemaText);

        // JSON -> AVRO -> JSON
        ByteArrayInputStream in = new ByteArrayInputStream(data.getBytes());
        GenericRecord record = JsonUtils.read(in, schema);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        AvroUtils.write(record, out);
        byte[] avro = out.toByteArray();

        in = new ByteArrayInputStream(avro);
        record = AvroUtils.read(in, schema);
        out = new ByteArrayOutputStream();
        JsonUtils.write(record, out);
        byte[] csv = out.toByteArray();
        assertEquals(data, new String(csv, StandardCharsets.UTF_8));
    }

    @Test
    @Parameters({"input_csv/union_null_last_field_with_default.txt,input_avro/union_and_matching_defaults.txt,expected_ouput_csv/union_null_last_field_with_default.txt",
            "input_csv/union_with_default.txt,input_avro/union_and_matching_defaults.txt,expected_ouput_csv/union_with_default.txt",
            "input_csv/union_null_middle_field_with_default.txt,input_avro/union_and_matching_defaults.txt,expected_ouput_csv/union_null_middle_field_with_default.txt",
            "input_csv/primitive_types.txt,input_avro/primitive_types_no_defaults.txt,expected_ouput_csv/primitive_types.txt",
            "input_csv/primitive_types_with_matching_default.txt,input_avro/primitive_types_with_matching_default.txt,expected_ouput_csv/primitive_types_with_matching_default.txt",
            "input_csv/decimal_logicalType_missing_value.txt,input_avro/decimal_logicalType_invalid_scale_with_default.txt,expected_ouput_csv/decimal_logicalType_with_default.txt"})
    public void testCSVRoundtrip(final String inputCSVFileName, final String inputAvroSchema, final String expectedOuput) throws Exception {
        final String data = getResourceAsString(inputCSVFileName);
        final String schemaText = getResourceAsString(inputAvroSchema);
        final String result = getResourceAsString(expectedOuput);
        csvRoundTrip(data, schemaText, result);
    }

    @Test
    @Parameters({"input_csv/union_with_missing_value.txt,input_avro/union_and_mismatch_defaults.txt",
            "input_csv/primitive_types_with_matching_default.txt,input_avro/primitive_types_with_mismatch_default.txt"})
    public void testCSVMismatchDefaults(final String inputCSVFileName, final String inputAvroSchema)  {
        try {
            final String data = getResourceAsString(inputCSVFileName);
            final String schemaText = getResourceAsString(inputAvroSchema);
            Schema schema = new Schema.Parser().parse(schemaText);

            ByteArrayInputStream in = new ByteArrayInputStream(data.getBytes());
            CSVUtils.read(in, '|', schema, '\"');
        }catch (IOException ioe){
            assertTrue(false);
        }catch(IllegalArgumentException iae){
            assertTrue(true);
        }
    }

    @Test
    public void testCSVRoundTrip() throws IOException {
        NumberFormat numberFormat = DecimalFormat.getInstance();
        numberFormat.setGroupingUsed(false);
        ((DecimalFormat) numberFormat).setParseBigDecimal(true);

        //"input_csv/decimal_logicalType.txt,input_avro/decimal_logicalType_invalid_scale_with_default.txt,expected_ouput_csv/decimal_logicalType_invalid_scale.txt",
        String decimalLogicalType = "\"fake_transactionid\"|" + numberFormat.format(new BigDecimal(11234567.89));
        String data = getResourceAsString("input_csv/decimal_logicalType.txt");
        String schemaText = getResourceAsString("input_avro/decimal_logicalType_invalid_scale_with_default.txt");
        csvRoundTrip(data, schemaText, decimalLogicalType);

        // needs to be set now because scale < precision
        numberFormat.setMaximumIntegerDigits(10);
        numberFormat.setMaximumFractionDigits(3);
        numberFormat.setMinimumFractionDigits(3);

        //"input_csv/decimal_logicalType.txt,input_avro/decimal_logicalType_valid_scale_with_no_default.txt,expected_ouput_csv/decimal_logicalType.txt",
        decimalLogicalType = "\"fake_transactionid\"|" + numberFormat.format(new BigDecimal(11234567.890));
        data = getResourceAsString("input_csv/decimal_logicalType.txt");
        schemaText = getResourceAsString("input_avro/decimal_logicalType_valid_scale_with_no_default.txt");

        //"input_csv/decimal_logicalType_missing_value.txt,input_avro/decimal_logicalType_valid_scale_with_default.txt,expected_ouput_csv/decimal_logicalType_valid_scale_with_default.txt",
        decimalLogicalType = "\"fake_transactionid\"|" + numberFormat.format(new BigDecimal(0.000));
        data = getResourceAsString("input_csv/decimal_logicalType_missing_value.txt");
        schemaText = getResourceAsString("input_avro/decimal_logicalType_valid_scale_with_default.txt");
        csvRoundTrip(data, schemaText, decimalLogicalType);
    }

    private void csvRoundTrip(final String data, final String schemaText, final String result) {
        Schema schema = new Schema.Parser().parse(schemaText);

        // CSV -> AVRO -> CSV
        ByteArrayInputStream in = new ByteArrayInputStream(data.getBytes());
        GenericRecord record = CSVUtils.read(in, '|', schema, '\"');
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        AvroUtils.write(record, out);
        byte[] avro = out.toByteArray();

        in = new ByteArrayInputStream(avro);
        record = AvroUtils.read(in, schema);
        out = new ByteArrayOutputStream();
        CSVUtils.write(record, '|', out);
        byte[] csv = out.toByteArray();
        assertEquals(result, new String(csv, StandardCharsets.UTF_8));
    }

    /**
     * Simple wrapper around getting the test resource file that is used by the above test cases
     *
     * @param fileName - the filename of the file to read
     * @return A string that contains the body of the file.
     * @throws IOException - if an error occurs reading the file.
     */
    private String getResourceAsString(String fileName) throws IOException {
        return new String(Files.readAllBytes(FileSystems.getDefault().getPath(classLoader.getResource(fileName).getPath())));
    }

}
