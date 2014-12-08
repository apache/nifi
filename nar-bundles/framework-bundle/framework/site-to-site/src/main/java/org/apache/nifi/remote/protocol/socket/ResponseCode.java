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
package org.apache.nifi.remote.protocol.socket;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.nifi.remote.exception.ProtocolException;


public enum ResponseCode {
    RESERVED(0, "Reserved for Future Use", false), // This will likely be used if we ever need to expand the length of
                                            // ResponseCode, so that we can indicate a 0 followed by some other bytes
    
    // handshaking properties
    PROPERTIES_OK(1, "Properties OK", false),
    UNKNOWN_PROPERTY_NAME(230, "Unknown Property Name", true),
    ILLEGAL_PROPERTY_VALUE(231, "Illegal Property Value", true),
    MISSING_PROPERTY(232, "Missing Property", true),
    
    // transaction indicators
    CONTINUE_TRANSACTION(10, "Continue Transaction", false),
    FINISH_TRANSACTION(11, "Finish Transaction", false),
    CONFIRM_TRANSACTION(12, "Confirm Transaction", true),   // "Explanation" of this code is the checksum
    TRANSACTION_FINISHED(13, "Transaction Finished", false),
    TRANSACTION_FINISHED_BUT_DESTINATION_FULL(14, "Transaction Finished But Destination is Full", false),
    BAD_CHECKSUM(19, "Bad Checksum", false),

    // data availability indicators
    MORE_DATA(20, "More Data Exists", false),
    NO_MORE_DATA(21, "No More Data Exists", false),
    
    // port state indicators
    UNKNOWN_PORT(200, "Unknown Port", false),
    PORT_NOT_IN_VALID_STATE(201, "Port Not in a Valid State", true),
    PORTS_DESTINATION_FULL(202, "Port's Destination is Full", false),
    
    // authorization
    UNAUTHORIZED(240, "User Not Authorized", true),
    
    // error indicators
    ABORT(250, "Abort", true),
    UNRECOGNIZED_RESPONSE_CODE(254, "Unrecognized Response Code", false),
    END_OF_STREAM(255, "End of Stream", false);
    
    private static final ResponseCode[] codeArray = new ResponseCode[256];
    
    static {
        for ( final ResponseCode responseCode : ResponseCode.values() ) {
            codeArray[responseCode.getCode()] = responseCode;
        }
    }
    
    private static final byte CODE_SEQUENCE_VALUE_1 = (byte) 'R';
    private static final byte CODE_SEQUENCE_VALUE_2 = (byte) 'C';
    private final int code;
    private final byte[] codeSequence;
    private final String description;
    private final boolean containsMessage;
    
    private ResponseCode(final int code, final String description, final boolean containsMessage) {
        this.codeSequence = new byte[] {CODE_SEQUENCE_VALUE_1, CODE_SEQUENCE_VALUE_2, (byte) code};
        this.code = code;
        this.description = description;
        this.containsMessage = containsMessage;
    }
    
    public int getCode() {
        return code;
    }
    
    public byte[] getCodeSequence() {
        return codeSequence;
    }
    
    @Override
    public String toString() {
        return description;
    }
    
    public boolean containsMessage() {
        return containsMessage;
    }
    
    public void writeResponse(final DataOutputStream out) throws IOException {
        if ( containsMessage() ) {
            throw new IllegalArgumentException("ResponseCode " + code + " expects an explanation");
        }
        
        out.write(getCodeSequence());
        out.flush();
    }
    
    public void writeResponse(final DataOutputStream out, final String explanation) throws IOException {
        if ( !containsMessage() ) {
            throw new IllegalArgumentException("ResponseCode " + code + " does not expect an explanation");
        }
        
        out.write(getCodeSequence());
        out.writeUTF(explanation);
        out.flush();
    }
    
    static ResponseCode readCode(final InputStream in) throws IOException, ProtocolException {
        final int byte1 = in.read();
        if ( byte1 < 0 ) {
            throw new EOFException();
        } else if ( byte1 != CODE_SEQUENCE_VALUE_1 ) {
            throw new ProtocolException("Expected to receive ResponseCode, but the stream did not have a ResponseCode");
        }
        
        final int byte2 = in.read();
        if ( byte2 < 0 ) {
            throw new EOFException();
        } else if ( byte2 != CODE_SEQUENCE_VALUE_2 ) {
            throw new ProtocolException("Expected to receive ResponseCode, but the stream did not have a ResponseCode");
        }

        final int byte3 = in.read();
        if ( byte3 < 0 ) {
            throw new EOFException();
        }
        
        final ResponseCode responseCode = codeArray[byte3];
        if (responseCode == null) {
            throw new ProtocolException("Received Response Code of " + byte3 + " but do not recognize this code");
        }
        return responseCode;
    }
    
    public static ResponseCode fromSequence(final byte[] value) {
        final int code = value[3] & 0xFF;
        final ResponseCode responseCode = codeArray[code];
        return (responseCode == null) ? UNRECOGNIZED_RESPONSE_CODE : responseCode;
    }
}