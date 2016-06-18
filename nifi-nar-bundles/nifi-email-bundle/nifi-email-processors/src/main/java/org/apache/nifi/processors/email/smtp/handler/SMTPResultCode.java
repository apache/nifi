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

package org.apache.nifi.processors.email.smtp.handler;

public enum SMTPResultCode {
    // This error isn't raised by code. Being just a default value for the
    // fromCode method below
    UNKNOWN_ERROR_CODE(0,
            "Unknown error.",
            "Failed due to unknown error"),

    SUCCESS (250,
            "Success delivering message",
            "Message from {} to {} via {} acknowledgement complete"),

    QUEUE_ERROR (421,
            "Could not queue the message. Try again",
            "The SMTP processor has just dropped a message due to the queue being too full, considering increasing the queue size" ),

    UNEXPECTED_ERROR(423,
            "Unexpected Error. Please try again or contact the administrator in case it persists",
            "Error hit during delivery of message from {}"),

    TIMEOUT_ERROR (451,
            "The processing of your message timed-out, we may have received it but you better off sending it again",
            "Message from {} to {} via {} acknowledgement timeout despite processing completed. Data duplication may occur"),

    MESSAGE_TOO_LARGE(500,
            "Message rejected due to length/size of data",
            "Your message exceeds the maximum permitted size");

    private static final SMTPResultCode[] codeArray = new SMTPResultCode[501];

    static {
        for (final SMTPResultCode smtpResultCode : SMTPResultCode.values()) {
            codeArray[smtpResultCode.getCode()] = smtpResultCode;
        }
    }

    private final int code;
    private final String errorMessage;
    private final String logMessage;

    SMTPResultCode(int code, String errorMessage, String logMessage) {
        this.code = code;
        this.errorMessage = errorMessage;
        this.logMessage = logMessage;
    }

    public int getCode() {
        return code;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public String getLogMessage() {
        return logMessage;
    }

    public static SMTPResultCode fromCode(int code) {
        final SMTPResultCode smtpResultCode = codeArray[code];
        return (smtpResultCode == null) ? UNKNOWN_ERROR_CODE : smtpResultCode;
    }


}
