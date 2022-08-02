package org.apache.iotdb.processors;

import com.alibaba.fastjson.JSON;
import org.apache.iotdb.processors.model.Schema;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.Tuple;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AbstractIoTDBUT {
    private static TestAbstractIoTDBProcessor processor;

    @BeforeAll
    public static void init() {
        processor = new TestAbstractIoTDBProcessor();
    }

    @Test
    public void testValidateSchemaAttribute() {
        // normal schema
        String schemaAttribute =
                "{\n"
                        + "\t\"timeType\": \"long\",\n"
                        + "\t\"fields\": [{\n"
                        + "\t\t\"tsName\": \"root.test_sg.test_d1.s1\",\n"
                        + "\t\t\"dataType\": \"INT32\",\n"
                        + "\t\t\"encoding\": \"RLE\"\n"
                        + "\t}, {\n"
                        + "\t\t\"tsName\": \"root.test_sg.test_d1.s2\",\n"
                        + "\t\t\"dataType\": \"DOUBLE\",\n"
                        + "\t\t\"encoding\": \"PLAIN\"\n"
                        + "\t}]\n"
                        + "}";

        Tuple<Boolean, String> tuple = processor.validateSchemaAttribute(schemaAttribute);
        Assert.assertTrue(tuple.getKey());
        Assert.assertEquals(null, tuple.getValue());

        // schema with wrong field
        schemaAttribute =
                "{\n"
                        + "\t\"time\": \"long\",\n"
                        + "\t\"fields\": [{\n"
                        + "\t\t\"tsName\": \"root.test_sg.test_d1.s1\",\n"
                        + "\t\t\"dataType\": \"INT32\",\n"
                        + "\t\t\"encoding\": \"RLE\"\n"
                        + "\t}, {\n"
                        + "\t\t\"tsName\": \"root.test_sg.test_d1.s2\",\n"
                        + "\t\t\"dataType\": \"DOUBLE\",\n"
                        + "\t\t\"encoding\": \"PLAIN\"\n"
                        + "\t}]\n"
                        + "}";
        tuple = processor.validateSchemaAttribute(schemaAttribute);
        String exceptedMsg = "The JSON of schema must contain `timeType` and `fields`.";

        Assert.assertEquals(false, tuple.getKey());
        Assert.assertEquals(exceptedMsg, tuple.getValue());

        // schema with wrong time type
        schemaAttribute =
                "{\n"
                        + "\t\"timeType\": \"int\",\n"
                        + "\t\"fields\": [{\n"
                        + "\t\t\"tsName\": \"root.test_sg.test_d1.s1\",\n"
                        + "\t\t\"dataType\": \"INT32\",\n"
                        + "\t\t\"encoding\": \"RLE\"\n"
                        + "\t}, {\n"
                        + "\t\t\"tsName\": \"root.test_sg.test_d1.s2\",\n"
                        + "\t\t\"dataType\": \"DOUBLE\",\n"
                        + "\t\t\"encoding\": \"PLAIN\"\n"
                        + "\t}]\n"
                        + "}";
        tuple = processor.validateSchemaAttribute(schemaAttribute);
        exceptedMsg =
                "Unknown `timeType`: int, there are only two options `long` and `string` for this property.";

        Assert.assertEquals(false, tuple.getKey());
        Assert.assertEquals(exceptedMsg, tuple.getValue());

        // schema without tsName
        schemaAttribute =
                "{\n"
                        + "\t\"timeType\": \"long\",\n"
                        + "\t\"fields\": [{\n"
                        + "\t\t\"dataType\": \"INT32\",\n"
                        + "\t\t\"encoding\": \"RLE\"\n"
                        + "\t}, {\n"
                        + "\t\t\"tsName\": \"root.test_sg.test_d1.s2\",\n"
                        + "\t\t\"dataType\": \"DOUBLE\",\n"
                        + "\t\t\"encoding\": \"PLAIN\"\n"
                        + "\t}]\n"
                        + "}";
        tuple = processor.validateSchemaAttribute(schemaAttribute);
        exceptedMsg = "`tsName` or `dataType` has not been set.";

        Assert.assertEquals(false, tuple.getKey());
        Assert.assertEquals(exceptedMsg, tuple.getValue());

        // schema without data type
        schemaAttribute =
                "{\n"
                        + "\t\"timeType\": \"long\",\n"
                        + "\t\"fields\": [{\n"
                        + "\t\t\"tsName\": \"root.test_sg.test_d1.s1\",\n"
                        + "\t\t\"encoding\": \"RLE\"\n"
                        + "\t}, {\n"
                        + "\t\t\"tsName\": \"root.test_sg.test_d1.s2\",\n"
                        + "\t\t\"dataType\": \"DOUBLE\",\n"
                        + "\t\t\"encoding\": \"PLAIN\"\n"
                        + "\t}]\n"
                        + "}";
        tuple = processor.validateSchemaAttribute(schemaAttribute);
        exceptedMsg = "`tsName` or `dataType` has not been set.";

        Assert.assertEquals(false, tuple.getKey());
        Assert.assertEquals(exceptedMsg, tuple.getValue());

        // tsName not start with root
        schemaAttribute =
                "{\n"
                        + "\t\"timeType\": \"long\",\n"
                        + "\t\"fields\": [{\n"
                        + "\t\t\"tsName\": \"test_sg.test_d1.s1\",\n"
                        + "\t\t\"dataType\": \"INT32\",\n"
                        + "\t\t\"encoding\": \"RLE\"\n"
                        + "\t}, {\n"
                        + "\t\t\"tsName\": \"root.test_sg.test_d1.s2\",\n"
                        + "\t\t\"dataType\": \"DOUBLE\",\n"
                        + "\t\t\"encoding\": \"PLAIN\"\n"
                        + "\t}]\n"
                        + "}";
        ;
        tuple = processor.validateSchemaAttribute(schemaAttribute);
        exceptedMsg = "The tsName `test_sg.test_d1.s1` is not start with 'root.'.";

        Assert.assertEquals(false, tuple.getKey());
        Assert.assertEquals(exceptedMsg, tuple.getValue());

        // schema with wrong data type
        schemaAttribute =
                "{\n"
                        + "\t\"timeType\": \"long\",\n"
                        + "\t\"fields\": [{\n"
                        + "\t\t\"tsName\": \"root.test_sg.test_d1.s1\",\n"
                        + "\t\t\"dataType\": \"INT\",\n"
                        + "\t\t\"encoding\": \"RLE\"\n"
                        + "\t}, {\n"
                        + "\t\t\"tsName\": \"root.test_sg.test_d1.s2\",\n"
                        + "\t\t\"dataType\": \"DOUBLE\",\n"
                        + "\t\t\"encoding\": \"PLAIN\"\n"
                        + "\t}]\n"
                        + "}";

        tuple = processor.validateSchemaAttribute(schemaAttribute);
        exceptedMsg =
                "Unknown `dataType`: INT. The supported dataTypes are [FLOAT, INT64, INT32, TEXT, DOUBLE, BOOLEAN]";

        Assert.assertEquals(false, tuple.getKey());
        Assert.assertEquals(exceptedMsg, tuple.getValue());

        // schema with wrong key
        schemaAttribute =
                "{\n"
                        + "\t\"timeType\": \"long\",\n"
                        + "\t\"fields\": [{\n"
                        + "\t\t\"tsName\": \"root.test_sg.test_d1.s1\",\n"
                        + "\t\t\"dataType\": \"INT32\",\n"
                        + "\t\t\"encode\": \"RLE\"\n"
                        + "\t}, {\n"
                        + "\t\t\"tsName\": \"root.test_sg.test_d1.s2\",\n"
                        + "\t\t\"dataType\": \"DOUBLE\",\n"
                        + "\t\t\"encoding\": \"PLAIN\"\n"
                        + "\t}]\n"
                        + "}";

        tuple = processor.validateSchemaAttribute(schemaAttribute);
        exceptedMsg = "Unknown property or properties: [encode]";

        Assert.assertEquals(false, tuple.getKey());
        Assert.assertEquals(exceptedMsg, tuple.getValue());

        // schema with wrong compression type
        schemaAttribute =
                "{\n"
                        + "\t\"timeType\": \"long\",\n"
                        + "\t\"fields\": [{\n"
                        + "\t\t\"tsName\": \"root.test_sg.test_d1.s1\",\n"
                        + "\t\t\"dataType\": \"INT32\",\n"
                        + "\t\t\"encoding\": \"RLE\",\n"
                        + "\t\t\"compressionType\": \"ZIP\"\n"
                        + "\t}, {\n"
                        + "\t\t\"tsName\": \"root.test_sg.test_d1.s2\",\n"
                        + "\t\t\"dataType\": \"DOUBLE\",\n"
                        + "\t\t\"encoding\": \"PLAIN\",\n"
                        + "\t\t\"compressionType\": \"GZIP\"\n"
                        + "\t}]\n"
                        + "}";

        tuple = processor.validateSchemaAttribute(schemaAttribute);
        exceptedMsg =
                "Unknown `compressionType`: ZIP, The supported compressionType are [LZO, PAA, SDT, UNCOMPRESSED, PLA, LZ4, GZIP, SNAPPY]";

        Assert.assertEquals(false, tuple.getKey());
        Assert.assertEquals(exceptedMsg, tuple.getValue());
    }

    @Test
    public void testParseSchema() {
        ArrayList<String> filedNames =
                new ArrayList<String>() {
                    {
                        add("root.sg1.d1.s1");
                        add("root.sg1.d1.s2");
                        add("root.sg1.d2.s1");
                    }
                };
        Map<String, List<String>> deviceMeasurementMap = processor.parseSchema(filedNames);
        HashMap<String, List<String>> exceptedMap =
                new HashMap<String, List<String>>() {
                    {
                        put(
                                "root.sg1.d1",
                                new ArrayList<String>() {
                                    {
                                        add("s1");
                                        add("s2");
                                    }
                                });
                        put(
                                "root.sg1.d2",
                                new ArrayList<String>() {
                                    {
                                        add("s1");
                                    }
                                });
                    }
                };
        Assert.assertEquals(exceptedMap, deviceMeasurementMap);
    }

    @Test
    public void testGenerateTablet() {
        String schemaAttribute =
                "{\n"
                        + "\t\"timeType\": \"long\",\n"
                        + "\t\"fields\": [{\n"
                        + "\t\t\"tsName\": \"root.test_sg.test_d1.s1\",\n"
                        + "\t\t\"dataType\": \"INT32\",\n"
                        + "\t\t\"encoding\": \"RLE\"\n"
                        + "\t}, {\n"
                        + "\t\t\"tsName\": \"root.test_sg.test_d1.s2\",\n"
                        + "\t\t\"dataType\": \"DOUBLE\",\n"
                        + "\t\t\"encoding\": \"PLAIN\"\n"
                        + "\t}]\n"
                        + "}";
        Schema schema = JSON.parseObject(schemaAttribute, Schema.class);
        HashMap<String, Tablet> generatedTablets = processor.generateTablets(schema, 1);

        HashMap<String, Tablet> exceptedTablets = new HashMap<>();
        ArrayList<MeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("s1", TSDataType.INT32, TSEncoding.RLE));
        schemas.add(new MeasurementSchema("s2", TSDataType.DOUBLE, TSEncoding.PLAIN));
        exceptedTablets.put("root.test_sg.test_d1", new Tablet("root.test_sg.test_d1", schemas, 1));

        Assert.assertEquals("root.test_sg.test_d1", generatedTablets.keySet().toArray()[0]);
        Assert.assertEquals(
                exceptedTablets.get("root.test_sg.test_d1").getSchemas(),
                generatedTablets.get("root.test_sg.test_d1").getSchemas());
        Assert.assertEquals(
                exceptedTablets.get("root.test_sg.test_d1").getMaxRowNumber(),
                generatedTablets.get("root.test_sg.test_d1").getMaxRowNumber());
        Assert.assertEquals(
                exceptedTablets.get("root.test_sg.test_d1").getTimeBytesSize(),
                generatedTablets.get("root.test_sg.test_d1").getTimeBytesSize());
        Assert.assertEquals(
                exceptedTablets.get("root.test_sg.test_d1").getTotalValueOccupation(),
                generatedTablets.get("root.test_sg.test_d1").getTotalValueOccupation());
        Assert.assertEquals(
                exceptedTablets.get("root.test_sg.test_d1").deviceId,
                generatedTablets.get("root.test_sg.test_d1").deviceId);
        Assert.assertEquals(
                exceptedTablets.get("root.test_sg.test_d1").rowSize,
                generatedTablets.get("root.test_sg.test_d1").rowSize);
    }

    public static class TestAbstractIoTDBProcessor extends AbstractIoTDB {

        @Override
        public void onTrigger(ProcessContext processContext, ProcessSession processSession)
                throws ProcessException {
        }
    }
}
