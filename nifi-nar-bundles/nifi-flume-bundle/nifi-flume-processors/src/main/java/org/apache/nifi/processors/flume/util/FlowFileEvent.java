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
package org.apache.nifi.processors.flume.util;

import static org.apache.nifi.processors.flume.util.FlowFileEventConstants.ENTRY_DATE_HEADER;
import static org.apache.nifi.processors.flume.util.FlowFileEventConstants.ID_HEADER;
import static org.apache.nifi.processors.flume.util.FlowFileEventConstants.LAST_QUEUE_DATE_HEADER;
import static org.apache.nifi.processors.flume.util.FlowFileEventConstants.LINEAGE_START_DATE_HEADER;
import static org.apache.nifi.processors.flume.util.FlowFileEventConstants.SIZE_HEADER;

import com.google.common.collect.Maps;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;
import org.apache.flume.Event;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.stream.io.StreamUtils;

public class FlowFileEvent implements Event {

  private final FlowFile flowFile;
  private final ProcessSession session;

  private final Map<String, String> headers;
  private boolean headersLoaded;

  private final Object bodyLock;
  private byte[] body;
  private boolean bodyLoaded;

  public FlowFileEvent(FlowFile flowFile, ProcessSession session) {
    this.flowFile = flowFile;
    this.session = session;

    headers = Maps.newHashMap();
    bodyLock = new Object();
    bodyLoaded = false;
  }

  @Override
  public Map<String, String> getHeaders() {
    synchronized (headers) {
      if (!headersLoaded) {
        headers.putAll(flowFile.getAttributes());
        headers.put(ENTRY_DATE_HEADER, Long.toString(flowFile.getEntryDate()));
        headers.put(ID_HEADER, Long.toString(flowFile.getId()));
        headers.put(LAST_QUEUE_DATE_HEADER, Long.toString(flowFile.getLastQueueDate()));
        headers.put(LINEAGE_START_DATE_HEADER, Long.toString(flowFile.getLineageStartDate()));
        headers.put(SIZE_HEADER, Long.toString(flowFile.getSize()));
        headersLoaded = true;
      }
    }
    return headers;
  }

  @Override
  public void setHeaders(Map<String, String> headers) {
    synchronized (this.headers) {
      this.headers.clear();
      this.headers.putAll(headers);
      headersLoaded = true;
    }
  }

  @Override
  public byte[] getBody() {
    synchronized (bodyLock) {
      if (!bodyLoaded) {
        if (flowFile.getSize() > Integer.MAX_VALUE) {
          throw new RuntimeException("Can't get body of Event because the backing FlowFile is too large (" + flowFile.getSize() + " bytes)");
        }

        final ByteArrayOutputStream baos = new ByteArrayOutputStream((int) flowFile.getSize());
        session.read(flowFile, new InputStreamCallback() {

          @Override
          public void process(InputStream in) throws IOException {
            try (BufferedInputStream input = new BufferedInputStream(in)) {
              StreamUtils.copy(input, baos);
            }
            baos.close();
          }
        });

        body = baos.toByteArray();
        bodyLoaded = true;
      }
    }

    return body;
  }

  @Override
  public void setBody(byte[] body) {
    synchronized (bodyLock) {
      this.body = Arrays.copyOf(body, body.length);
      bodyLoaded = true;
    }
  }
}
