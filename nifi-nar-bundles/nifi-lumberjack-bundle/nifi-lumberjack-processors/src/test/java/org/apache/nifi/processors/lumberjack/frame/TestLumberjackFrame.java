package org.apache.nifi.processors.lumberjack.frame;

import org.junit.Test;

import static org.junit.Assert.*;


public class TestLumberjackFrame {

    @Test(expected = LumberjackFrameException.class)
    public void testInvalidVersion() {
        new LumberjackFrame.Builder().seqNumber(1234).dataSize(3).build();
    }
    @Test(expected = LumberjackFrameException.class)
    public void testInvalidFrameType() {
        new LumberjackFrame.Builder().frameType((byte) 0x70).dataSize(5).build();
    }

    @Test(expected = LumberjackFrameException.class)
    public void testBlankFrameType() {
        new LumberjackFrame.Builder().frameType(((byte) 0x00)).dataSize(5).build();
    }
}