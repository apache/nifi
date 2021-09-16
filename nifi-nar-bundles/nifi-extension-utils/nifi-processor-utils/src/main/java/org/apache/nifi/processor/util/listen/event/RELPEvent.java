package org.apache.nifi.processor.util.listen.event;

public class RELPEvent extends StandardNettyEvent {
    //private final RELPFrame frame;

//    public RELPEvent(final String sender, final RELPFrame frame) {
//        super(sender, frame.getData());
//        this.frame = frame;
//    }

    public RELPEvent(final String sender, final byte[] data) {
        super(sender, data);
//        this.frame = frame;
    }

//    public RELPFrame getFrame() {
//        return frame;
//    }

    public String getCommand() {
        return "commandlol";
    }
}