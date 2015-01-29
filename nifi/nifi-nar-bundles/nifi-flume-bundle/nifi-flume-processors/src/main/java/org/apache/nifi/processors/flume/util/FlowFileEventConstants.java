
package org.apache.nifi.processors.flume.util;


public class FlowFileEventConstants {

  // FlowFile#getEntryDate();
  public static final String ENTRY_DATE_HEADER = "nifi.entry.date";

  // FlowFile#getId();
  public static final String ID_HEADER = "nifi.id";

  // FlowFile#getLastQueueDate();
  public static final String LAST_QUEUE_DATE_HEADER = "nifi.last.queue.date";

  // FlowFile#getLineageIdentifiers();
  public static final String LINEAGE_IDENTIFIERS_HEADER = "nifi.lineage.identifiers";

  // FlowFile#getLineageStartDate();
  public static final String LINEAGE_START_DATE_HEADER = "nifi.lineage.start.date";

  // FlowFile#getSize();
  public static final String SIZE_HEADER = "nifi.size";

}
