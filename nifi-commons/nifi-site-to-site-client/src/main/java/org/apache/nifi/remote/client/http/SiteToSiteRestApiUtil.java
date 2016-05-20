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

import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.exception.PortNotRunningException;
import org.apache.nifi.remote.exception.ProtocolException;
import org.apache.nifi.remote.exception.UnknownPortException;
import org.apache.nifi.remote.io.http.HttpCommunicationsSession;
import org.apache.nifi.remote.io.http.HttpInput;
import org.apache.nifi.remote.io.http.HttpOutput;
import org.apache.nifi.remote.protocol.CommunicationsSession;
import org.apache.nifi.remote.protocol.http.HttpHeaders;
import org.apache.nifi.remote.protocol.ResponseCode;
import org.apache.nifi.remote.util.NiFiRestApiUtil;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.web.api.dto.remote.PeerDTO;
import org.apache.nifi.web.api.entity.PeersEntity;
import org.apache.nifi.web.api.entity.TransactionResultEntity;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.util.Collection;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.HANDSHAKE_PROPERTY_BATCH_COUNT;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.HANDSHAKE_PROPERTY_BATCH_DURATION;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.HANDSHAKE_PROPERTY_BATCH_SIZE;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.HANDSHAKE_PROPERTY_REQUEST_EXPIRATION;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.HANDSHAKE_PROPERTY_USE_COMPRESSION;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.LOCATION_HEADER_NAME;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.LOCATION_URI_INTENT_NAME;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.LOCATION_URI_INTENT_VALUE;

public class SiteToSiteRestApiUtil extends NiFiRestApiUtil {

    private static final Logger logger = LoggerFactory.getLogger(SiteToSiteRestApiUtil.class);
    private HttpURLConnection urlConnection;

    private boolean compress = false;
    private int requestExpirationMillis = 0;
    private int batchCount = 0;
    private long batchSize = 0;
    private long batchDurationMillis = 0;
    private TransportProtocolVersionNegotiator transportProtocolVersionNegotiator = new TransportProtocolVersionNegotiator(1);

    public SiteToSiteRestApiUtil(SSLContext sslContext, Proxy proxy) {
        super(sslContext, proxy);
    }

    public Collection<PeerDTO> getPeers() throws IOException {
        urlConnection = getConnection("/site-to-site/peers");
        urlConnection.setDoOutput(false);
        urlConnection.setRequestMethod("GET");
        urlConnection.setRequestProperty("Accept", "application/json");
        urlConnection.setRequestProperty(HttpHeaders.PROTOCOL_VERSION, String.valueOf(transportProtocolVersionNegotiator.getVersion()));

        return getEntity(urlConnection, PeersEntity.class).getPeers();
    }

    public String initiateTransaction(TransferDirection direction, String portId) throws IOException {
        if (TransferDirection.RECEIVE.equals(direction)) {
            return initiateTransaction("output-ports", portId);
        } else {
            return initiateTransaction("input-ports", portId);
        }
    }

    private String initiateTransaction(String portType, String portId) throws IOException {
        logger.debug("initiateTransaction handshaking portType={}, portId={}", portType, portId);
        urlConnection = getConnection("/site-to-site/" + portType + "/" + portId + "/transactions");
        urlConnection.setDoOutput(false);
        urlConnection.setRequestMethod("POST");
        urlConnection.setRequestProperty("Accept", "application/json");
        urlConnection.setRequestProperty(HttpHeaders.PROTOCOL_VERSION, String.valueOf(transportProtocolVersionNegotiator.getVersion()));

        setHandshakeProperties();

        int responseCode = urlConnection.getResponseCode();
        logger.debug("initiateTransaction responseCode={}", responseCode);

        String transactionUrl;
        switch (responseCode) {
            case RESPONSE_CODE_CREATED :
                transactionUrl = readTransactionUrl();
                if (isEmpty(transactionUrl)) {
                    throw new ProtocolException("Server returned RESPONSE_CODE_CREATED without Location header");
                }
                String protocolVersionConfirmedByServerStr = urlConnection.getHeaderField(HttpHeaders.PROTOCOL_VERSION);
                if (isEmpty(protocolVersionConfirmedByServerStr)) {
                    throw new ProtocolException("Server didn't return confirmed protocol version");
                }
                Integer protocolVersionConfirmedByServer = Integer.valueOf(protocolVersionConfirmedByServerStr);
                logger.debug("Finished version negotiation, protocolVersionConfirmedByServer={}", protocolVersionConfirmedByServer);
                transportProtocolVersionNegotiator.setVersion(protocolVersionConfirmedByServer);
                break;

            default:
                throw handleErrResponse(responseCode);
        }
        logger.debug("initiateTransaction handshaking finished, transactionUrl={}", transactionUrl);
        return transactionUrl;
    }

    public void openConnectionForSend(String transactionUrl, CommunicationsSession commSession) throws IOException {

        urlConnection = getConnection(transactionUrl + "/flow-files");
        urlConnection.setDoOutput(true);
        urlConnection.setRequestMethod("POST");
        urlConnection.setRequestProperty("Content-Type", "application/octet-stream");
        urlConnection.setRequestProperty("Accept", "text/plain");
        urlConnection.setInstanceFollowRedirects(false);
        urlConnection.setRequestProperty(HttpHeaders.PROTOCOL_VERSION, String.valueOf(transportProtocolVersionNegotiator.getVersion()));

        setHandshakeProperties();

        ((HttpOutput)commSession.getOutput()).setOutputStream(urlConnection.getOutputStream());

    }

    private void setHandshakeProperties() {
        if(compress) urlConnection.setRequestProperty(HANDSHAKE_PROPERTY_USE_COMPRESSION, "true");
        if(requestExpirationMillis > 0) urlConnection.setRequestProperty(HANDSHAKE_PROPERTY_REQUEST_EXPIRATION, String.valueOf(requestExpirationMillis));
        if(batchCount > 0) urlConnection.setRequestProperty(HANDSHAKE_PROPERTY_BATCH_COUNT, String.valueOf(batchCount));
        if(batchSize > 0) urlConnection.setRequestProperty(HANDSHAKE_PROPERTY_BATCH_SIZE, String.valueOf(batchSize));
        if(batchDurationMillis > 0) urlConnection.setRequestProperty(HANDSHAKE_PROPERTY_BATCH_DURATION, String.valueOf(batchDurationMillis));
    }

    public boolean openConnectionForReceive(String transactionUrl, CommunicationsSession commSession) throws IOException {

        urlConnection = getConnection(transactionUrl + "/flow-files");
        urlConnection.setRequestMethod("GET");
        urlConnection.setRequestProperty("Accept", "application/octet-stream");
        urlConnection.setInstanceFollowRedirects(false);
        urlConnection.setRequestProperty(HttpHeaders.PROTOCOL_VERSION, String.valueOf(transportProtocolVersionNegotiator.getVersion()));

        setHandshakeProperties();

        int responseCode = urlConnection.getResponseCode();
        logger.debug("responseCode={}", responseCode);

        switch (responseCode) {
            case RESPONSE_CODE_OK :
                logger.debug("Server returned RESPONSE_CODE_OK, indicating there was no data.");
                return false;

            case RESPONSE_CODE_ACCEPTED :
                ((HttpInput)commSession.getInput()).setInputStream(urlConnection.getInputStream());
                return true;

            default:
                throw handleErrResponse(responseCode);
        }
    }

    private IOException handleErrResponse(int responseCode) throws IOException {
        InputStream in = urlConnection.getErrorStream();
        if(in == null) {
            return new IOException("Unexpected response code: " + responseCode);
        }
        TransactionResultEntity errEntity = readResponse(in);
        ResponseCode errCode = ResponseCode.fromCode(errEntity.getResponseCode());
        switch (errCode) {
            case UNKNOWN_PORT:
                return new UnknownPortException(errEntity.getMessage());
            case PORT_NOT_IN_VALID_STATE:
                return new PortNotRunningException(errEntity.getMessage());
            default:
                return new IOException("Unexpected response code: " + responseCode
                        + " errCode:" + errCode + " errMessage:" + errEntity.getMessage());
        }
    }

    private String readTransactionUrl() {
        final String locationUriIntentHeader = urlConnection.getHeaderField(LOCATION_URI_INTENT_NAME);
        logger.debug("locationUriIntentHeader={}", locationUriIntentHeader);
        if (locationUriIntentHeader != null) {
            if (LOCATION_URI_INTENT_VALUE.equals(locationUriIntentHeader)) {
                String transactionUrl = urlConnection.getHeaderField(LOCATION_HEADER_NAME);
                logger.debug("transactionUrl={}", transactionUrl);
                return transactionUrl;
            }
        }
        return null;
    }

    public void finishTransferFlowFiles(CommunicationsSession commSession) throws IOException {

        commSession.getOutput().getOutputStream().flush();

        int responseCode = urlConnection.getResponseCode();

        switch (responseCode) {
            case RESPONSE_CODE_ACCEPTED :
                ((HttpInput)commSession.getInput()).setInputStream(urlConnection.getInputStream());
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                StreamUtils.copy(commSession.getInput().getInputStream(), bos);
                String receivedChecksum = bos.toString("UTF-8");
                ((HttpCommunicationsSession)commSession).setChecksum(receivedChecksum);
                logger.debug("receivedChecksum={}", receivedChecksum);
                break;


            default:
                throw handleErrResponse(responseCode);
        }

    }

    public TransactionResultEntity commitReceivingFlowFiles(String transactionUrl, String checksum) throws IOException {
        logger.debug("Sending commitReceivingFlowFiles request to transactionUrl: {}, checksum=", transactionUrl, checksum);

        urlConnection = getConnection(transactionUrl + "?checksum=" + checksum);
        urlConnection.setRequestMethod("DELETE");
        urlConnection.setRequestProperty("Accept", "application/json");
        urlConnection.setRequestProperty(HttpHeaders.PROTOCOL_VERSION, String.valueOf(transportProtocolVersionNegotiator.getVersion()));

        setHandshakeProperties();

        int responseCode = urlConnection.getResponseCode();
        logger.debug("commitReceivingFlowFiles responseCode={}", responseCode);


        switch (responseCode) {
            case RESPONSE_CODE_OK :
                return readResponse(urlConnection.getInputStream());

            case RESPONSE_CODE_BAD_REQUEST :
                return readResponse(urlConnection.getErrorStream());

            default:
                throw handleErrResponse(responseCode);
        }

    }

    private TransactionResultEntity readResponse(InputStream inputStream) throws IOException {

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        StreamUtils.copy(inputStream, bos);
        String responseMessage = null;
        try {
            responseMessage = new String(bos.toByteArray(), "UTF-8");
            logger.debug("readResponse responseMessage={}", responseMessage);

            final ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(responseMessage, TransactionResultEntity.class);

        } catch (JsonParseException | JsonMappingException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Failed to parse JSON.", e);
            }
            TransactionResultEntity entity = new TransactionResultEntity();
            entity.setResponseCode(ResponseCode.ABORT.getCode());
            entity.setMessage(responseMessage);
            return entity;
        }
    }

    public TransactionResultEntity commitTransferFlowFiles(String transactionUrl, ResponseCode clientResponse) throws IOException {
        String requestUrl = transactionUrl + "?responseCode=" + clientResponse.getCode();
        logger.debug("Sending commitTransferFlowFiles request to transactionUrl: {}", requestUrl);

        urlConnection = getConnection(requestUrl);
        urlConnection.setRequestMethod("DELETE");
        urlConnection.setRequestProperty("Accept", "application/json");
        urlConnection.setRequestProperty(HttpHeaders.PROTOCOL_VERSION, String.valueOf(transportProtocolVersionNegotiator.getVersion()));

        setHandshakeProperties();

        int responseCode = urlConnection.getResponseCode();
        logger.debug("commitTransferFlowFiles responseCode={}", responseCode);

        switch (responseCode) {
            case RESPONSE_CODE_OK :
                return readResponse(urlConnection.getInputStream());

            case RESPONSE_CODE_BAD_REQUEST :
                return readResponse(urlConnection.getErrorStream());

            default:
                throw handleErrResponse(responseCode);
        }

    }

    public void setCompress(boolean compress) {
        this.compress = compress;
    }

    public void setRequestExpirationMillis(int requestExpirationMillis) {
        if(requestExpirationMillis < 0) throw new IllegalArgumentException("requestExpirationMillis can't be a negative value.");
        this.requestExpirationMillis = requestExpirationMillis;
    }

    public void setBatchCount(int batchCount) {
        if(batchCount < 0) throw new IllegalArgumentException("batchCount can't be a negative value.");
        this.batchCount = batchCount;
    }

    public void setBatchSize(long batchSize) {
        if(batchSize < 0) throw new IllegalArgumentException("batchSize can't be a negative value.");
        this.batchSize = batchSize;
    }

    public void setBatchDurationMillis(long batchDurationMillis) {
        if(batchDurationMillis < 0) throw new IllegalArgumentException("batchDurationMillis can't be a negative value.");
        this.batchDurationMillis = batchDurationMillis;
    }

    public Integer getTransactionProtocolVersion() {
        return transportProtocolVersionNegotiator.getTransactionProtocolVersion();
    }
}
