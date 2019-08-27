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
package org.apache.nifi.metrics.reporting.reporter.service;


import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.json.MetricsModule;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

//import org.apache.nifi.reporting.util.metrics.api.MetricsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.HttpsURLConnection;
import javax.xml.bind.DatatypeConverter;

import java.io.DataOutputStream;
import java.net.InetAddress;
import java.net.ProtocolException;
import java.net.URL;
import java.nio.charset.Charset;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 * AzureLogAnalysticsReporter is a ScheduleReporter that sends metrics to Azure Log Analystics Workspace.
 * For reference, look at <a href="https://docs.microsoft.com/en-us/azure/azure-monitor/platform/data-collector-api">Azure documentation</a>. 
 * @author Seokwon J. Yang
 */
public class AzureLogAnalysticsReporter extends ScheduledReporter {

    private static final Logger logger = LoggerFactory.getLogger(AzureLogAnalysticsReporter.class);
    private final ObjectMapper mapper;
    private final String workspaceId;
    private final String workspaceKey;
    private final String logType;

    /**
     * AzureLogAnalysticsReporter Constructor used by {@link AzureLogAnalysticsMetricReporterService}
     * 
     * @param workspaceId  Azure log analystics workspace id, retreived from the adavancned setting
     * @param workspaceKey Azure log analystics workspace key (either primary or secondary), retreived from the adavancned setting
     * @param logType Record type name (<a hef="https://docs.microsoft.com/en-us/azure/azure-monitor/platform/data-collector-api#record-type-and-properties">Ref</>)
     * @param registry registry with the metrics to report
     * @param filter metric filter. By default, all
     * @param rateUnit rate unite. By default, TimeUnit.MILLISECONDS
     * @param durationUnit duration unit. By default, TimeUnit.MILLISECONDS
     */
    public AzureLogAnalysticsReporter(
        String workspaceId,
        String workspaceKey,
        String logType,
        MetricRegistry registry, MetricFilter filter, TimeUnit rateUnit, TimeUnit durationUnit)
    {
        
        super(registry, "AzureLogAnalysticsReporter", filter, rateUnit, durationUnit);
        MetricsModule metricsModule = new MetricsModule(rateUnit, durationUnit, false, filter);

        mapper = new ObjectMapper().registerModule(metricsModule);
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        this.workspaceId = workspaceId;
        this.workspaceKey = workspaceKey;
        this.logType = logType;
    }
    /**
     * InjectComputer column and value to json in string format and return the result
     * @param json
     * @return json in string with computer column and value
     */
    private String injectComputerName(String json) {
        String result ="";
        try {
            String hostname= InetAddress.getLocalHost().getHostName();
            ObjectMapper _mapper = new ObjectMapper();
            Map<String, Object> map = _mapper.readValue(json, new TypeReference<HashMap<String, Object>>(){});
            if(map.size() > 0) {
                map.put("Computer", hostname);
                result = _mapper.writeValueAsString(map);
            }

        }catch(Exception e) {
            e.printStackTrace();
            logger.debug(e.getMessage());
        }
        return result;
    }

    /**
     * Implemenation of report method for Azure log analystics
     */
	@Override
	public void report(
        SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
        SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) 
    {

        try {
            // collect all metrics values into batch list to send together
            List<String> batch = new ArrayList<String>(5);

            Object[] array = new Object[] { gauges, counters, histograms, meters, timers };
            // go thru one by one
            for(Object metric: array) {
                String json = mapper.writeValueAsString(metric).trim();
                //logger.debug(json);
                String injected = "";
                if(json.length() > 0) {
                    injected = injectComputerName(json);
                    logger.debug(injected);
                    if(injected.length() > 0) {
                        batch.add(injected);
                    }

                }
            }
            if(batch.size() >0 ){
                // only if there are batch to send, collect into a list in json
                String result = String.join(",", batch);
                result = "[" + result + "]";
                
                logger.debug(result);

                if(this.workspaceId != null &&  !this.workspaceId.isEmpty()) {
                    sendToAzureLogAnalystics(result);
                }
            }
        }
        catch(JsonProcessingException e) {
            logger.error(e.getMessage());
        }
    }

    private static final String HMAC_SHA256_ALG = "HmacSHA256";
    private static final Charset UTF8 = Charset.forName("UTF-8");
    /**
     * geneate authoriization code from content length and rfc1123Data
     * @param contentLength
     * @param rfc1123Date
     * @return
     */
    private String createAuthorization(int contentLength, String rfc1123Date) {
        
        try {
            // Documentation: https://docs.microsoft.com/en-us/rest/api/loganalytics/create-request
            String signature = String.format(
                "POST\n%d\napplication/json\nx-ms-date:%s\n/api/logs", contentLength, rfc1123Date);
            Mac mac = Mac.getInstance(AzureLogAnalysticsReporter.HMAC_SHA256_ALG);
            mac.init(
                new SecretKeySpec(
                    DatatypeConverter.parseBase64Binary(this.workspaceKey), 
                    AzureLogAnalysticsReporter.HMAC_SHA256_ALG
                )
            );
            String hmac = DatatypeConverter.printBase64Binary(
                mac.doFinal(signature.getBytes(AzureLogAnalysticsReporter.UTF8))
            );
            return String.format("SharedKey %s:%s", this.workspaceId, hmac);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }
    /**
     * send json object in string to azure log analystics
     * @param jdata
     */
    public void sendToAzureLogAnalystics(String jdata)
    {
        String dataCollectorEndpoint = String.format(
            "https://%s.ods.opinsights.azure.com/api/logs?api-version=2016-04-01", this.workspaceId);
        
        HttpsURLConnection conn;
        try {
            URL url = new URL(dataCollectorEndpoint);
            conn = (HttpsURLConnection) url.openConnection();
        } catch (Exception e1) {
            e1.printStackTrace();
            logger.debug(e1.getMessage());
            return;
		}
        TimeZone tz = TimeZone.getTimeZone("UTC");
		DateFormat df = new SimpleDateFormat("YYYY-MM-DD'T'hh:mm:ssZ");
		df.setTimeZone(tz);
		String nowAsISO = df.format(new Date());

		SimpleDateFormat fmt = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss");
		fmt.setTimeZone(TimeZone.getTimeZone("GMT"));
        String date = fmt.format(Calendar.getInstance().getTime()) + " GMT";

        try {
            conn.setRequestMethod("POST");
        } catch (ProtocolException e) {
            e.printStackTrace();
            logger.debug(e.getMessage());
            return;
        }
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty("Log-Type", this.logType);
        conn.setRequestProperty("x-ms-date", date);
        conn.setRequestProperty("Authorization", createAuthorization(jdata.length(), date));
        conn.addRequestProperty("time-generated-field", nowAsISO);

        try {
            DataOutputStream wr = new DataOutputStream(conn.getOutputStream());
			wr.writeBytes(jdata);
			wr.flush();
			wr.close();
			int responseCode = conn.getResponseCode();
			logger.debug("\nSending 'POST' request to URL : " + dataCollectorEndpoint);
			logger.debug("Post parameters : " + jdata);
			logger.debug("Response Code : " + responseCode);
        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
            return;
        }
    }



}