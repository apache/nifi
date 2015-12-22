package org.apache.nifi.processors.elasticsearch;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.*;


import java.io.IOException;
import java.io.InputStream;

public class TestPutElasticSearch {

    private InputStream twitterExample;
    private TestRunner runner;

    @Before
    public void setUp() throws IOException {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        twitterExample = classloader
                .getResourceAsStream("TweetExample.json");

    }

    @After
    public void teardown() {
        runner = null;

    }


    @Test
    @Ignore("Comment this out if you want to run against local or test ES")
    public void testPutElasticSearchBasic() throws IOException {
        System.out.println("Starting test " + new Object() {
        }.getClass().getEnclosingMethod().getName());
        final TestRunner runner = TestRunners.newTestRunner(new PutElasticSearch());
        runner.setValidateExpressionUsage(false);
        //Local Cluster - Mac pulled from brew
        runner.setProperty(AbstractElasticSearchProcessor.CLUSTER_NAME, "elasticsearch_brew");
        runner.setProperty(AbstractElasticSearchProcessor.HOSTS, "127.0.0.1:9300");
        runner.setProperty(AbstractElasticSearchProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticSearchProcessor.SAMPLER_INTERVAL, "5s");
        runner.setProperty(AbstractElasticSearchProcessor.INDEX_STRATEGY, "Monthly");
        runner.setProperty(PutElasticSearch.BATCH_SIZE, "1");


        runner.enqueue(twitterExample);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutElasticSearch.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PutElasticSearch.REL_SUCCESS).get(0);


    }

    @Test
    @Ignore("Comment this out if you want to run against local or test ES")
    public void testPutElasticSearchBatch() throws IOException {
        System.out.println("Starting test " + new Object() {
        }.getClass().getEnclosingMethod().getName());
        final TestRunner runner = TestRunners.newTestRunner(new PutElasticSearch());
        runner.setValidateExpressionUsage(false);
        //Local Cluster - Mac pulled from brew
        runner.setProperty(AbstractElasticSearchProcessor.CLUSTER_NAME, "elasticsearch_brew");
        runner.setProperty(AbstractElasticSearchProcessor.HOSTS, "127.0.0.1:9300");
        runner.setProperty(AbstractElasticSearchProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticSearchProcessor.SAMPLER_INTERVAL, "5s");
        runner.setProperty(AbstractElasticSearchProcessor.INDEX_STRATEGY, "Monthly");
        runner.setProperty(PutElasticSearch.BATCH_SIZE, "100");

        JsonParser parser = new JsonParser();
        JsonObject json;
        String message = convertStreamToString(twitterExample);
        for (int i = 0;i < 100; i++){

            json = parser.parse(message).getAsJsonObject();
            String id = json.get("id").getAsString();
            long newId = Long.parseLong(id) + i;
            json.addProperty("id", newId);
            runner.enqueue(message.getBytes());

        }

        runner.run();

        runner.assertAllFlowFilesTransferred(PutElasticSearch.REL_SUCCESS, 100);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PutElasticSearch.REL_SUCCESS).get(0);

    }

    /**
     * Convert an input stream to a stream
     * @param is input the input stream
     * @return return the converted input stream as a string
     */
    static String convertStreamToString(java.io.InputStream is) {
        java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
        return s.hasNext() ? s.next() : "";
    }
}
