package org.apache.iotdb.processors;

import com.alibaba.fastjson.JSON;
import org.apache.iotdb.processors.model.Schema;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

public class PutIoTDBUT {

    @Test
    public void testParseSchemaByAttribute() {
        String schemaAttribute =
                "{\n"
                        + "\t\"timeType\": \"long\",\n"
                        + "\t\"fields\": [\n"
                        + "\t\t{\"tsName\": \"root.sg.d1.s1\",\"dataType\": \"INT32\", \"encoding\": \"PLAIN\", \"compressionType\": \"SNAPPY\"},\n"
                        + "\t\t{\"tsName\": \"root.sg.d1.s2\",\"dataType\": \"BOOLEAN\", \"encoding\": \"PLAIN\", \"compressionType\": \"GZIP\"},\n"
                        + "\t\t{\"tsName\": \"root.sg.d1.s3\",\"dataType\": \"TEXT\", \"encoding\": \"DICTIONARY\"}\n"
                        + "\t]\n"
                        + "}";
        Schema.TimeType exceptedTimeType = Schema.TimeType.LONG;
        ArrayList<String> exceptedFieldNames =
                new ArrayList<String>() {
                    {
                        add("root.sg.d1.s1");
                        add("root.sg.d1.s2");
                        add("root.sg.d1.s3");
                    }
                };
        ArrayList<TSDataType> exceptedDataTypes =
                new ArrayList<TSDataType>() {
                    {
                        add(TSDataType.INT32);
                        add(TSDataType.BOOLEAN);
                        add(TSDataType.TEXT);
                    }
                };
        ArrayList<TSEncoding> exceptedEncodings =
                new ArrayList<TSEncoding>() {
                    {
                        add(TSEncoding.PLAIN);
                        add(TSEncoding.PLAIN);
                        add(TSEncoding.DICTIONARY);
                    }
                };

        ArrayList<CompressionType> exceptedCompressionTypes =
                new ArrayList<CompressionType>() {
                    {
                        add(CompressionType.SNAPPY);
                        add(CompressionType.GZIP);
                        add(null);
                    }
                };

        Schema schema = JSON.parseObject(schemaAttribute, Schema.class);
        Assert.assertEquals(exceptedTimeType, schema.getTimeType());
        Assert.assertEquals(exceptedFieldNames, schema.getFieldNames());
        Assert.assertEquals(exceptedDataTypes, schema.getDataTypes());
        Assert.assertEquals(exceptedEncodings, schema.getEncodingTypes());
        Assert.assertEquals(exceptedCompressionTypes, schema.getCompressionTypes());
    }
}
