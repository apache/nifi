/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.influxdb;

import com.google.gson.JsonObject;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.apache.commons.lang3.RandomUtils;
import org.apache.nifi.influxdb.serialization.InfluxLineProtocolReader;
import org.apache.nifi.influxdb.services.InfluxDBService;
import org.apache.nifi.influxdb.services.StandardInfluxDBService;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.MockProcessorInitializationContext;
import org.apache.nifi.util.TestRunners;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT_PROPERTY;
import static org.hamcrest.MatcherAssert.assertThat;

public class ITPutInfluxDBRecord extends AbstractITInfluxDB {


    private MockRecordParser recordReader;
    private PutInfluxDBRecord processor;

    @Before
    public void setUp() throws Exception {

        dbName = "testRecord";

        initInfluxDB();

        processor = new PutInfluxDBRecord();

        runner = TestRunners.newTestRunner(processor);
        runner.setProperty(PutInfluxDBRecord.DB_NAME, dbName);
        runner.setProperty(PutInfluxDBRecord.MEASUREMENT, "testRecordMeasurement");
        runner.setProperty(PutInfluxDBRecord.RECORD_READER_FACTORY, "recordReader");
        runner.setProperty(PutInfluxDBRecord.INFLUX_DB_SERVICE, "influxdb-service");

        recordReader = new MockRecordParser();
        runner.addControllerService("recordReader", recordReader);
        runner.enableControllerService(recordReader);

        InfluxDBService influxDBService = new StandardInfluxDBService();
        runner.addControllerService("influxdb-service", influxDBService);
        runner.enableControllerService(influxDBService);

        MockProcessContext context = new MockProcessContext(processor);
        MockProcessorInitializationContext initContext = new MockProcessorInitializationContext(processor, context);
        processor.initialize(initContext);
    }

    @Test
    public void storeComplexData() throws ParseException {

        runner.setProperty(PutInfluxDBRecord.TAGS, "lang,keyword");
        runner.setProperty(PutInfluxDBRecord.TIMESTAMP_FIELD, "timestamp");
        runner.setProperty(PutInfluxDBRecord.FIELDS,
                "retweet_count,tweet_id,followers_count,screen_name,friends_count,favourites_count,user_verified,raw");

        recordReader.addSchemaField("lang", RecordFieldType.STRING);
        recordReader.addSchemaField("keyword", RecordFieldType.STRING);
        recordReader.addSchemaField("retweet_count", RecordFieldType.INT);
        recordReader.addSchemaField("tweet_id", RecordFieldType.STRING);
        recordReader.addSchemaField("followers_count", RecordFieldType.INT);
        recordReader.addSchemaField("screen_name", RecordFieldType.STRING);
        recordReader.addSchemaField("friends_count", RecordFieldType.INT);
        recordReader.addSchemaField("favourites_count", RecordFieldType.INT);
        recordReader.addSchemaField("user_verified", RecordFieldType.BOOLEAN);
        recordReader.addSchemaField("raw", RecordFieldType.STRING);
        recordReader.addSchemaField("timestamp", RecordFieldType.LONG);

        Date timeValue = new Date();
        recordReader.addRecord(
                "en", "crypto", 10, "123456789id", 15, "my name", 5, 20, false, "crypto rules!", timeValue);

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutInfluxDBRecord.REL_SUCCESS, 1);

        List<QueryResult.Result> results = runQuery("SELECT * FROM testRecordMeasurement GROUP BY *").getResults();

        Assert.assertEquals(1, results.size());
        Assert.assertEquals(1, results.get(0).getSeries().size());
        QueryResult.Series series = results.get(0).getSeries().get(0);

        Assert.assertEquals("testRecordMeasurement", series.getName());
        Assert.assertEquals(2, series.getTags().size());
        Assert.assertEquals("en", series.getTags().get("lang"));
        Assert.assertEquals("crypto", series.getTags().get("keyword"));

        Assert.assertEquals(9, series.getColumns().size());
        Assert.assertEquals(1, series.getValues().size());

        List<Object> values = series.getValues().get(0);

        assertThat(values.get(series.getColumns().indexOf("raw")), new ComparableMatcher("crypto rules!"));
        assertThat(values.get(series.getColumns().indexOf("retweet_count")), new ComparableMatcher(10.0));
        assertThat(values.get(series.getColumns().indexOf("tweet_id")), new ComparableMatcher("123456789id"));
        assertThat(values.get(series.getColumns().indexOf("followers_count")), new ComparableMatcher(15d));
        assertThat(values.get(series.getColumns().indexOf("screen_name")), new ComparableMatcher("my name"));
        assertThat(values.get(series.getColumns().indexOf("friends_count")), new ComparableMatcher(5d));
        assertThat(values.get(series.getColumns().indexOf("favourites_count")), new ComparableMatcher(20d));
        assertThat(values.get(series.getColumns().indexOf("user_verified")), new ComparableMatcher(false));

        double time = (double) values.get(series.getColumns().indexOf("time"));
        double millis = (double) TimeUnit.NANOSECONDS.convert(timeValue.getTime(), TimeUnit.MILLISECONDS);

        assertThat(time, new ComparableMatcher(millis));
    }

    @Test
    public void fieldTypeConflict() throws InitializationException {

        runner.setProperty(PutInfluxDBRecord.TAGS, "success");
        runner.setProperty(PutInfluxDBRecord.FIELDS, "orderTime,healthy");

        recordReader.addSchemaField("success", RecordFieldType.STRING);
        recordReader.addSchemaField("orderTime", RecordFieldType.DATE);
        recordReader.addSchemaField("healthy", RecordFieldType.BOOLEAN);

        recordReader.addRecord(Boolean.TRUE, new Date(), Boolean.TRUE);

        runner.enqueue("");
        runner.run();

        // success save first record
        runner.assertTransferCount(PutInfluxDBRecord.REL_SUCCESS, 1);

        // create second record
        recordReader = new MockRecordParser();
        runner.addControllerService("recordReader", recordReader);
        runner.enableControllerService(recordReader);

        // wrong type
        recordReader.addSchemaField("success", RecordFieldType.STRING);
        recordReader.addSchemaField("orderTime", RecordFieldType.BOOLEAN);
        recordReader.addSchemaField("healthy", RecordFieldType.DATE);

        recordReader.addRecord(Boolean.TRUE, Boolean.TRUE, new Date());

        runner.enqueue("");
        runner.run();

        runner.assertTransferCount(PutInfluxDBRecord.REL_FAILURE, 1);
    }


    @Test
    public void parallelRunningProcessor() throws InitializationException {

        String schema = "{\n"
                + "  \"name\": \"putInfluxDBRecord\",\n"
                + "  \"namespace\": \"org.influxdb\",\n"
                + "  \"type\": \"record\",\n"
                + "  \"fields\": [\n"
                + "    { \"name\": \"car_type\", \"type\": \"string\" },\n"
                + "    { \"name\": \"loc_x\", \"type\": \"int\" },\n"
                + "    { \"name\": \"loc_y\", \"type\": \"int\" }\n"
                + "  ]\n"
                + "}";

        String data = "{\"car_type\" : \"taxi\", \"loc_x\" : 15, \"loc_y\" : 18}";

        JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("jsonReader", jsonReader);

        runner.setProperty(jsonReader, SCHEMA_ACCESS_STRATEGY, SCHEMA_TEXT_PROPERTY.getValue());
        runner.setProperty(jsonReader, SCHEMA_TEXT, schema);
        runner.setProperty(PutInfluxDBRecord.RECORD_READER_FACTORY, "jsonReader");

        runner.setProperty(PutInfluxDBRecord.TAGS, "car_type");
        runner.setProperty(PutInfluxDBRecord.FIELDS, "loc_x,loc_y");

        runner.enableControllerService(jsonReader);

        List<String> carTypes = new ArrayList<>();
        carTypes.add("taxi");
        carTypes.add("bus");
        carTypes.add("submarine");
        carTypes.add("airplane");
        carTypes.add("ship");

        MockProcessSession processSession = (MockProcessSession) runner.getProcessSessionFactory().createSession();

        int count = 100;
        MockFlowFile[] flowFiles = IntStream.range(0, count).mapToObj(i -> {

            String carType = carTypes.get(RandomUtils.nextInt(0, 5));

            JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("car_type", carType);
            jsonObject.addProperty("loc_x", RandomUtils.nextInt());
            jsonObject.addProperty("loc_y", RandomUtils.nextInt());

            return processSession.createFlowFile(data.getBytes());

        }).toArray(MockFlowFile[]::new);

        runner.enqueue(flowFiles);
        runner.setThreadCount(10);
        runner.run(count);

        runner.assertTransferCount(PutInfluxDBRecord.REL_SUCCESS, count);

        List<QueryResult.Result> results = runQuery("select count(*) from testRecordMeasurement").getResults();

        List<Object> values = results.get(0).getSeries().get(0).getValues().get(0);

        assertThat(values.get(1), new ComparableMatcher((double) count));
        assertThat(values.get(2), new ComparableMatcher((double) count));
    }

    @Test
    public void batching() {

        // batch by 50
        runner.setProperty(PutInfluxDBRecord.ENABLE_BATCHING, "true");
        runner.setProperty(PutInfluxDBRecord.BATCH_ACTIONS, "50");

        runner.setProperty(PutInfluxDBRecord.TAGS, "botanical_category");
        runner.setProperty(PutInfluxDBRecord.FIELDS, "total_size,delta_size");

        recordReader.addSchemaField("botanical_category", RecordFieldType.STRING);
        recordReader.addSchemaField("total_size", RecordFieldType.INT);
        recordReader.addSchemaField("delta_size", RecordFieldType.INT);

        IntStream.range(0, 10).forEach(index -> recordReader.addRecord("lignotuber_" + index, 100 + index, index));

        // run processor
        runner.enqueue("");
        // without close
        runner.run(1, false);

        runner.assertTransferCount(PutInfluxDBRecord.REL_SUCCESS, 1);

        List<QueryResult.Result> results = runQuery("select * from flowers_grow GROUP BY total_size").getResults();
        Assert.assertEquals(1, results.size());
        Assert.assertNull(results.get(0).getSeries());

        // flush
        processor.close();
        results = runQuery("select * from testRecordMeasurement GROUP BY total_size").getResults();

        QueryResult.Series flowersGrowSeries = results.get(0).getSeries().get(0);
        Assert.assertEquals(10, flowersGrowSeries.getValues().size());
    }

    @Test
    public void saveInfluxLineProtocol() throws InitializationException {

        String data = "weather,location=us-midwest temperature=82 1465839830100400200";

        InfluxLineProtocolReader readerFactory = new InfluxLineProtocolReader();

        runner.addControllerService("inline-reader", readerFactory);
        runner.setProperty(readerFactory, InfluxLineProtocolReader.CHARSET, StandardCharsets.UTF_8.name());
        runner.enableControllerService(readerFactory);

        runner.setProperty(PutInfluxDBRecord.RECORD_READER_FACTORY, "inline-reader");
        runner.setProperty(PutInfluxDBRecord.TAGS, "tags");
        runner.setProperty(PutInfluxDBRecord.FIELDS, "fields");
        runner.setProperty(PutInfluxDBRecord.TIMESTAMP_FIELD, "timestamp");

        runner.enqueue(data);
        runner.run();

        runner.assertTransferCount(PutInfluxDBRecord.REL_SUCCESS, 1);

        List<QueryResult.Result> results = runQuery("select * from testRecordMeasurement GROUP BY *").getResults();

        QueryResult.Series series = results.get(0).getSeries().get(0);

        Assert.assertEquals("testRecordMeasurement", series.getName());
        Assert.assertEquals(1, series.getTags().size());
        Assert.assertEquals("us-midwest", series.getTags().get("location"));

        Assert.assertEquals(2, series.getColumns().size());
        Assert.assertEquals(1, series.getValues().size());

        List<Object> values = series.getValues().get(0);

        assertThat(values.get(series.getColumns().indexOf("temperature")), new ComparableMatcher(82D));

        double time = (double) values.get(series.getColumns().indexOf("time"));
        assertThat(time, new ComparableMatcher(1465839830100400200D));
    }

    @NonNull
    private QueryResult runQuery(@NonNull final String command) {

        QueryResult result = influxDB.query(new Query(command, dbName), TimeUnit.NANOSECONDS);

        Assert.assertFalse(result.hasError());

        return result;
    }

    private final class ComparableMatcher extends BaseMatcher<Object> {

        private Comparable expected;

        private ComparableMatcher(final Comparable expected) {
            this.expected = expected;
        }


        @Override
        public boolean matches(final Object item) {

            //noinspection unchecked
            return expected.compareTo(item) == 0;
        }

        @Override
        public void describeTo(final Description description) {

            description.appendValue(expected);
        }
    }
}
