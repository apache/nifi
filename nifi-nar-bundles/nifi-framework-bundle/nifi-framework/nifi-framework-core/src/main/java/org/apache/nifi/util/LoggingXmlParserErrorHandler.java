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
package org.apache.nifi.util;

import org.slf4j.Logger;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * ErrorHandler implementation for Logging XML schema validation errors
 */
public class LoggingXmlParserErrorHandler extends DefaultHandler {

    private final Logger logger;
    private final String xmlDocTitle;
    private static final String MESSAGE_FORMAT = "Schema validation %s parsing %s at line %d, col %d: %s";

    public LoggingXmlParserErrorHandler(String xmlDocTitle, Logger logger) {
        this.logger = logger;
        this.xmlDocTitle = xmlDocTitle;
    }

    @Override
    public void error(final SAXParseException err) throws SAXParseException {
        String message = String.format(MESSAGE_FORMAT, "error", xmlDocTitle, err.getLineNumber(),
                err.getColumnNumber(), err.getMessage());
        logger.warn(message);
    }
}