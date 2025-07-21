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
package org.apache.nifi.web;

import java.io.IOException;
import java.io.InputStream;

/**
 * Interface for obtaining content from the NiFi content repository.
 */
public interface ViewableContent {

    String CONTENT_REQUEST_ATTRIBUTE = "org.apache.nifi.web.content";

    enum DisplayMode {

        Original,
        Formatted,
        Hex;
    }

    /**
     * @return stream to the viewable content. The data stream can only be read once
     * so an extension can call this method or getContent
     */
    InputStream getContentStream();

    /**
     * @return the content as a string. The data stream can only be read once so an
     * extension can call this method or getContentStream
     * @throws java.io.IOException if unable to read content
     */
    String getContent() throws IOException;

    /**
     * @return the desired display mode. If the mode is Hex the framework will
     * handle generating the mark up. The only values that an extension will see
     * is Original or Formatted
     */
    DisplayMode getDisplayMode();

    /**
     * @return contents file name
     */
    String getFileName();

    /**
     * @return mime type of the content, value is lowercase and stripped of all parameters if there were any
     */
    String getContentType();

    /**
     * @return unchanged mime type of the content
     */
    String getRawContentType();

}
