package org.apache.nifi.controller.queue.clustered.protocol;

public class LoadBalanceProtocolConstants {
    // Protocol negotiation constants
    public static final int VERSION_ACCEPTED = 0x10;
    public static final int REQEUST_DIFFERENT_VERSION = 0x11;
    public static final int ABORT_PROTOCOL_NEGOTIATION = 0x12;

    // Transaction constants
    public static final int CONFIRM_CHECKSUM = 0x21;
    public static final int REJECT_CHECKSUM = 0x22;
    public static final int COMPLETE_TRANSACTION = 0x23;
    public static final int ABORT_TRANSACTION = 0x24;
    public static final int CONFIRM_COMPLETE_TRANSACTION = 0x25;

    // FlowFile constants
    public static final int MORE_FLOWFILES = 0x31;
    public static final int NO_MORE_FLOWFILES = 0x32;

    // data frame constants
    public static final int NO_DATA_FRAME = 0x40;
    public static final int DATA_FRAME_FOLLOWS = 0x42;
}
