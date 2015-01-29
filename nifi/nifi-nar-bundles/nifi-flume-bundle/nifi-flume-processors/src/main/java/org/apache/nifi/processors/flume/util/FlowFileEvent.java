
package org.apache.nifi.processors.flume.util;

import com.google.common.collect.Maps;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;
import org.apache.flume.Event;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.InputStreamCallback;

import static org.apache.nifi.processors.flume.util.FlowFileEventConstants.*;
import org.apache.nifi.stream.io.BufferedInputStream;
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
    if (!headersLoaded) {
      synchronized (headers) {
        if (headersLoaded) {
          return headers;
        }

        headers.putAll(flowFile.getAttributes());
        headers.put(ENTRY_DATE_HEADER, Long.toString(flowFile.getEntryDate()));
        headers.put(ID_HEADER, Long.toString(flowFile.getId()));
        headers.put(LAST_QUEUE_DATE_HEADER, Long.toString(flowFile.getLastQueueDate()));
        int i = 0;
        for (String lineageIdentifier : flowFile.getLineageIdentifiers()) {
          headers.put(LINEAGE_IDENTIFIERS_HEADER + "." + i, lineageIdentifier);
          i++;
        }
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
    if (bodyLoaded) {
      return body;
    }

    synchronized (bodyLock ) {
      if (!bodyLoaded) {
        if (flowFile.getSize() > Integer.MAX_VALUE) {
          throw new RuntimeException("Can't get body of Event because the backing FlowFile is too large (" + flowFile.getSize() + " bytes)");
        }
    
        final ByteArrayOutputStream baos = new ByteArrayOutputStream((int) flowFile.getSize());
        session.read(flowFile, new InputStreamCallback() {

          @Override
          public void process(InputStream in) throws IOException {
            try (BufferedInputStream input = new BufferedInputStream(in)) {
              StreamUtils.copy(in, baos);
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
