package org.apache.nifi.util;

import org.slf4j.Marker;

public class LogMessage {
    private final Marker marker;
    private final String msg;
    private final Throwable throwable;
    private final Object[] args;
    public LogMessage(final Marker marker, final String msg, final Throwable t, final Object... args) {
        this.marker = marker;
        this.msg = msg;
        this.throwable = t;
        this.args = args;
    }
    
    public Marker getMarker() {
        return marker;
    }
    public String getMsg() {
        return msg;
    }
    public Throwable getThrowable() {
        return throwable;
    }
    public Object[] getArgs() {
        return args;
    }
}