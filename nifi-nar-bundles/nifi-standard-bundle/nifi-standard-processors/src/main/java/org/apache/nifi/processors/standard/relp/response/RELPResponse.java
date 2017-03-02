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
package org.apache.nifi.processors.standard.relp.response;

import org.apache.nifi.processors.standard.relp.frame.RELPFrame;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

/**
 * The data portion of a RELPFrame for a response:
 *
 * RSP-CODE [SP HUMANMSG] LF [CMDDATA]
 *
 */
public class RELPResponse {

    public static final int OK = 200;
    public static final int ERROR = 500;

    public static final String RSP_CMD = "rsp";

    private final long txnr;
    private final int code;
    private final String message;
    private final String data;

    public RELPResponse(final long txnr, final int code) {
        this(txnr, code, null, null);
    }

    public RELPResponse(final long txnr, final int code, final String message, final String data) {
        this.txnr = txnr;
        this.code = code;
        this.message = message;
        this.data = data;
    }

    /**
     * Creates a RELPFrame where the data portion will contain this response.
     *
     * @param charset the character set to encode the response
     *
     * @return a RELPFrame for for this response
     */
    public RELPFrame toFrame(final Charset charset) {
        final StringBuilder builder = new StringBuilder();
        builder.append(code);

        if (message != null && !message.isEmpty()) {
            builder.append((char)RELPFrame.SEPARATOR);
            builder.append(message);
        }

        if (data != null) {
            builder.append((char)RELPFrame.DELIMITER);
            builder.append(data);
        }

        final byte[] data = builder.toString().getBytes(charset);

        return new RELPFrame.Builder()
                .txnr(txnr).command(RSP_CMD)
                .dataLength(data.length).data(data)
                .build();
    }

    /**
     * Utility method to create a response to an open request.
     *
     * @param txnr the transaction number of the open request
     * @param offers the accepted offers
     *
     * @return the RELPResponse for the given open request
     */
    public static RELPResponse open(final long txnr, final Map<String,String> offers) {
        int i = 0;
        final StringBuilder sb = new StringBuilder();
        for (final Map.Entry<String, String> entry : offers.entrySet()) {
            if (i > 0) {
                sb.append((char)RELPFrame.DELIMITER);
            }

            sb.append(entry.getKey());

            if (entry.getValue() != null) {
                sb.append('=');
                sb.append(entry.getValue());
            }
            i++;
        }

        return new RELPResponse(txnr, OK, "OK", sb.toString());
    }

    /**
     * Utility method to create a default "OK" response.
     *
     * @param txnr the transaction number being responded to
     *
     * @return a RELPResponse with a 200 code and a message of "OK"
     */
    public static RELPResponse ok(final long txnr) {
        return new RELPResponse(txnr, OK, "OK", null);
    }

    /**
     * Utility method to create a default "ERROR" response.
     *
     * @param txnr the transaction number being responded to
     *
     * @return a RELPResponse with a 500 code and a message of "ERROR"
     */
    public static RELPResponse error(final long txnr) {
        return new RELPResponse(txnr, ERROR, "ERROR", null);
    }


    /**
     * Parses the provided data into a Map of offers.
     *
     * @param data the data portion of a RELPFrame for an "open" command
     * @param charset the charset to decode the data
     *
     * @return a Map of offers, or an empty Map if no data is provided
     */
    public static Map<String,String> parseOffers(final byte[] data, final Charset charset) {
        final Map<String, String> offers = new HashMap<>();
        if (data == null || data.length == 0) {
            return offers;
        }

        final String dataStr = new String(data, charset);
        final String[] splits = dataStr.split("[" + (char)RELPFrame.DELIMITER + "]");

        for (final String split : splits) {
            final String[] fields = split.split( "=", 2);
            if (fields.length > 1 ) {
                offers.put(fields[0], fields[1]);
            } else {
                offers.put(fields[0], fields[0]);
            }
        }

        return offers;
    }
}
