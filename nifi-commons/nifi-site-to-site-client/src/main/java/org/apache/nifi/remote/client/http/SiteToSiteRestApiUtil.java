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
package org.apache.nifi.remote.client.http;

import org.apache.nifi.remote.exception.ProtocolException;
import org.apache.nifi.remote.io.http.HttpCommunicationsSession;
import org.apache.nifi.remote.io.http.HttpInput;
import org.apache.nifi.remote.io.http.HttpOutput;
import org.apache.nifi.remote.protocol.CommunicationsSession;
import org.apache.nifi.remote.protocol.socket.ResponseCode;
import org.apache.nifi.remote.util.NiFiRestApiUtil;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.web.api.dto.remote.PeerDTO;
import org.apache.nifi.web.api.entity.PeersEntity;
import org.apache.nifi.web.api.entity.TransactionResultEntity;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.Collection;

public class SiteToSiteRestApiUtil extends NiFiRestApiUtil {

    private static final Logger logger = LoggerFactory.getLogger(SiteToSiteRestApiUtil.class);
    private HttpURLConnection urlConnection;

    public static final String LOCATION_HEADER_NAME = "Location";
    public static final String LOCATION_URI_INTENT_NAME = "x-location-uri-intent";
    public static final String LOCATION_URI_INTENT_VALUE = "flowfile-hold";

    public SiteToSiteRestApiUtil(SSLContext sslContext) {
        super(sslContext);
    }

    public Collection<PeerDTO> getPeers() throws IOException {
        return getEntity("/site-to-site/peers", PeersEntity.class).getPeers();
    }

    public void openConnectionForSend(String portId, CommunicationsSession commSession) throws IOException {
        logger.debug("openConnectionForSend to port: {}", portId);

        urlConnection = getConnection("/site-to-site/ports/" + portId + "/flow-files");
        urlConnection.setDoOutput(true);
        urlConnection.setRequestMethod("POST");
        urlConnection.setRequestProperty("Content-Type", "application/octet-stream");
        urlConnection.setRequestProperty("Accept", "text/plain");
        urlConnection.setInstanceFollowRedirects(false);

        ((HttpOutput)commSession.getOutput()).setOutputStream(urlConnection.getOutputStream());

    }

    public String openConnectionForReceive(String portId, CommunicationsSession commSession) throws IOException {
        logger.debug("openConnectionForSend to port: {}", portId);
        urlConnection = getConnection("/site-to-site/ports/" + portId + "/flow-files");
        urlConnection.setRequestMethod("GET");
        urlConnection.setRequestProperty("Accept", "application/octet-stream");
        urlConnection.setInstanceFollowRedirects(false);

        ((HttpInput)commSession.getInput()).setInputStream(urlConnection.getInputStream());

        int responseCode = urlConnection.getResponseCode();
        logger.debug("responseCode={}", responseCode);

        if (responseCode == RESPONSE_CODE_OK) {
            // Although server tries to send 204 when it doesn't have data, since data is already exchanged, it returns 200 instead.
            logger.debug("Server returned RESPONSE_CODE_OK, indicating there was no data.");
            return null;

        } else if (responseCode == RESPONSE_CODE_SEE_OTHER) {
            String holdUri = getHoldUri();
            if (holdUri != null) return holdUri;

            throw new ProtocolException("Server returned RESPONSE_CODE_SEE_OTHER without Location header");

        } else {
            throw new IOException("Unexpected response code: " + responseCode + " response body:" + readErrResponse());
        }

    }

    private String getHoldUri() {
        final String locationUriIntentHeader = urlConnection.getHeaderField(LOCATION_URI_INTENT_NAME);
        logger.debug("Received 303: locationUriIntentHeader={}", locationUriIntentHeader);
        if (locationUriIntentHeader != null) {
            if (LOCATION_URI_INTENT_VALUE.equals(locationUriIntentHeader)) {
                String holdUri = urlConnection.getHeaderField(LOCATION_HEADER_NAME);
                logger.debug("Received 303: holdUri={}", holdUri);
                return holdUri;
            }
        }
        return null;
    }

    public String finishTransferFlowFiles(CommunicationsSession commSession) throws IOException {

        commSession.getOutput().getOutputStream().flush();

        int responseCode = urlConnection.getResponseCode();
        if (responseCode == RESPONSE_CODE_SEE_OTHER) {
            String holdUri = getHoldUri();
            if (holdUri != null) {
                ((HttpInput)commSession.getInput()).setInputStream(urlConnection.getInputStream());
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                StreamUtils.copy(commSession.getInput().getInputStream(), bos);
                String receivedChecksum = bos.toString("UTF-8");
                ((HttpCommunicationsSession)commSession).setChecksum(receivedChecksum);
                logger.debug("receivedChecksum={}", receivedChecksum);
                return holdUri;
            }

            throw new ProtocolException("Server returned 303 without Location header");

        } else {
            throw new IOException("Unexpected response code: " + responseCode + " response body:" + readErrResponse());
        }

    }

    public TransactionResultEntity commitReceivingFlowFiles(String holdUri, String checksum) throws IOException {
        logger.debug("Sending commitReceivingFlowFiles request to holdUri: {}, checksum=", holdUri, checksum);

        urlConnection = getConnection(holdUri + "?checksum=" + checksum);
        urlConnection.setRequestMethod("DELETE");
        urlConnection.setRequestProperty("Accept", "application/json");


        int responseCode = urlConnection.getResponseCode();
        logger.debug("commitReceivingFlowFiles responseCode={}", responseCode);


        if (responseCode == RESPONSE_CODE_OK) {
            return readResponse(urlConnection.getInputStream());
        } else if (responseCode == RESPONSE_CODE_BAD_REQUEST) {
            return readResponse(urlConnection.getErrorStream());
        } else {
            throw new IOException("Unexpected response code: " + responseCode + " response body:" + readErrResponse());
        }
    }

    private TransactionResultEntity readResponse(InputStream inputStream) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        StreamUtils.copy(inputStream, bos);
        String responseMessage = new String(bos.toByteArray(), "UTF-8");
        logger.debug("commitReceivingFlowFiles responseMessage={}", responseMessage);

        final ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(responseMessage, TransactionResultEntity.class);
    }

    private String readErrResponse() {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            StreamUtils.copy(urlConnection.getErrorStream(), bos);
            String responseMessage = new String(bos.toByteArray(), "UTF-8");
            return responseMessage;
        } catch (Throwable t){
            logger.warn("Failed to read error response.", t.getMessage());
            return null;
        }
    }

    public TransactionResultEntity commitTransferFlowFiles(String holdUri, ResponseCode clientResponse) throws IOException {
        String requestUrl = holdUri + "?responseCode=" + clientResponse.getCode();
        logger.debug("Sending commitTransferFlowFiles request to holdUri: {}", requestUrl);

        urlConnection = getConnection(requestUrl);
        urlConnection.setRequestMethod("DELETE");
        urlConnection.setRequestProperty("Accept", "application/json");

        int responseCode = urlConnection.getResponseCode();
        logger.debug("commitTransferFlowFiles responseCode={}", responseCode);

        if (responseCode == RESPONSE_CODE_OK) {
            return readResponse(urlConnection.getInputStream());
        } else if (responseCode == RESPONSE_CODE_BAD_REQUEST) {
            return readResponse(urlConnection.getErrorStream());
        } else {
            throw new IOException("Unexpected response code: " + responseCode + " response body:" + readErrResponse());
        }
    }

}
