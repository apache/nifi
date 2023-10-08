package org.apache.nifi.processors.doris;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class DorisStreamLoad {

    private final static String DORIS_HOST = "xxx.com";
    private final static String DORIS_DB = "test";
    private final static String DORIS_TABLE = "stream_test";
    private final static String DORIS_USER = "root";
    private final static String DORIS_PASSWORD = "xxx";
    private final static int DORIS_HTTP_PORT = 8410;

    private void sendData(String content) throws Exception {
        final String loadUrl = String.format("http://%s:%s/api/%s/%s/_stream_load",
                DORIS_HOST,
                DORIS_HTTP_PORT,
                DORIS_DB,
                DORIS_TABLE);

        final HttpClientBuilder httpClientBuilder = HttpClients
                .custom()
                .setRedirectStrategy(new DefaultRedirectStrategy() {
                    @Override
                    protected boolean isRedirectable(String method) {
                        return true;
                    }
                });

        try (CloseableHttpClient client = httpClientBuilder.build()) {
            HttpPut put = new HttpPut(loadUrl);
            StringEntity entity = new StringEntity(content, "UTF-8");
            put.setHeader(HttpHeaders.EXPECT, "100-continue");
            put.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader(DORIS_USER, DORIS_PASSWORD));
            // the label header is optional, not necessary
            // use label header can ensure at most once semantics
            put.setHeader("label", "39c25a5c-7000-496e-a98e-348a264c81de");
            put.setEntity(entity);

            try (CloseableHttpResponse response = client.execute(put)) {
                String loadResult = "";
                if (response.getEntity() != null) {
                    loadResult = EntityUtils.toString(response.getEntity());
                }
                final int statusCode = response.getStatusLine().getStatusCode();
                // statusCode 200 just indicates that doris be service is ok, not stream load
                // you should see the output content to find whether stream load is success
                if (statusCode != 200) {
                    throw new IOException(
                            String.format("Stream load failed, statusCode=%s load result=%s", statusCode, loadResult));
                }

                System.out.println(loadResult);
            }
        }
    }

    private String basicAuthHeader(String username, String password) {
        final String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }

    public static void main(String[] args) throws Exception {
        int id1 = 1;
        int id2 = 10;
        String id3 = "张三";
        int rowNumber = 10;
        String oneRow = id1 + "\t" + id2 + "\t" + id3 + "\n";

        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < rowNumber; i++) {
            stringBuilder.append(oneRow);
        }

        //in doris 0.9 version, you need to comment this line
        //refer to https://github.com/apache/doris/issues/783
        stringBuilder.deleteCharAt(stringBuilder.length() - 1);

        String loadData = stringBuilder.toString();
        System.out.println("loadData = " + loadData);
        DorisStreamLoad dorisStreamLoad = new DorisStreamLoad();
        dorisStreamLoad.sendData(loadData);
    }
}
