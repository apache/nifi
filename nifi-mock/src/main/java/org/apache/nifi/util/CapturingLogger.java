package org.apache.nifi.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.Marker;

/**
 * Implementation of SLF4J logger that records every log message and provides a mechanism
 * to retrieve them. 
 *
 */
public class CapturingLogger implements Logger {

    private final Logger logger;
    
    private List<LogMessage> traceMessages = new ArrayList<>();
    private List<LogMessage> debugMessages = new ArrayList<>();
    private List<LogMessage> infoMessages = new ArrayList<>();
    private List<LogMessage> warnMessages = new ArrayList<>();
    private List<LogMessage> errorMessages = new ArrayList<>();

    public CapturingLogger(final Logger logger) {
        this.logger = logger;
    }
    public List<LogMessage> getTraceMessages() {
        return Collections.unmodifiableList(traceMessages);
    }
    public List<LogMessage> getDebugMessages() {
        return Collections.unmodifiableList(debugMessages);
    }
    public List<LogMessage> getInfoMessages() {
        return Collections.unmodifiableList(infoMessages);
    }
    public List<LogMessage> getWarnMessages() {
        return Collections.unmodifiableList(warnMessages);
    }
    public List<LogMessage> getErrorMessages() {
        return Collections.unmodifiableList(errorMessages);
    }
    
    @Override
    public String getName() {
        return logger.getName();
    }

    @Override
    public boolean isTraceEnabled() {
        return logger.isTraceEnabled();
    }

    @Override
    public void trace(String msg) {
        traceMessages.add(new LogMessage(null, msg, null));
        logger.trace(msg);
    }

    @Override
    public void trace(String format, Object arg) {
        traceMessages.add(new LogMessage(null, format, null, arg));
        logger.trace(format, arg);
    }

    @Override
    public void trace(String format, Object arg1, Object arg2) {
        traceMessages.add(new LogMessage(null, format, null, arg1, arg2));
        logger.trace(format, arg1, arg2);
    }

    @Override
    public void trace(String format, Object... arguments) {
        traceMessages.add(new LogMessage(null, format, null, arguments));
        logger.trace(format, arguments);
    }

    @Override
    public void trace(String msg, Throwable t) {
        traceMessages.add(new LogMessage(null, msg, t));
        logger.trace(msg, t);
    }

    @Override
    public boolean isTraceEnabled(Marker marker) {
        return logger.isTraceEnabled(marker);
    }

    @Override
    public void trace(Marker marker, String msg) {
        traceMessages.add(new LogMessage(marker, msg, null));
        logger.trace(marker, msg);

    }

    @Override
    public void trace(Marker marker, String format, Object arg) {
        traceMessages.add(new LogMessage(marker, format, null, arg));
        logger.trace(marker, format, arg);

    }

    @Override
    public void trace(Marker marker, String format, Object arg1, Object arg2) {
        traceMessages.add(new LogMessage(marker, format, null, arg1, arg2));
        logger.trace(marker, format, arg1, arg2);
    }

    @Override
    public void trace(Marker marker, String format, Object... argArray) {
        traceMessages.add(new LogMessage(marker, format, null, argArray));
        logger.trace(marker, format, argArray);
    }

    @Override
    public void trace(Marker marker, String msg, Throwable t) {
        traceMessages.add(new LogMessage(marker, msg, t));
        logger.trace(marker, msg, t);
    }

    // DEBUG
    @Override
    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }

    @Override
    public void debug(String msg) {
        debugMessages.add(new LogMessage(null, msg, null));
        logger.debug(msg);
    }

    @Override
    public void debug(String format, Object arg) {
        debugMessages.add(new LogMessage(null, format, null, arg));
        logger.debug(format, arg);
    }

    @Override
    public void debug(String format, Object arg1, Object arg2) {
        debugMessages.add(new LogMessage(null, format, null, arg1, arg2));
        logger.debug(format, arg1, arg2);
    }

    @Override
    public void debug(String format, Object... arguments) {
        debugMessages.add(new LogMessage(null, format, null, arguments));
        logger.debug(format, arguments);
    }

    @Override
    public void debug(String msg, Throwable t) {
        debugMessages.add(new LogMessage(null, msg, t));
        logger.debug(msg, t);
    }

    @Override
    public boolean isDebugEnabled(Marker marker) {
        return logger.isDebugEnabled(marker);
    }

    @Override
    public void debug(Marker marker, String msg) {
        debugMessages.add(new LogMessage(marker, msg, null));
        logger.debug(marker, msg);

    }

    @Override
    public void debug(Marker marker, String format, Object arg) {
        debugMessages.add(new LogMessage(marker, format, null, arg));
        logger.debug(marker, format, arg);

    }

    @Override
    public void debug(Marker marker, String format, Object arg1, Object arg2) {
        debugMessages.add(new LogMessage(marker, format, null, arg1, arg2));
        logger.debug(marker, format, arg1, arg2);
    }

    @Override
    public void debug(Marker marker, String format, Object... argArray) {
        debugMessages.add(new LogMessage(marker, format, null, argArray));
        logger.debug(marker, format, argArray);
    }

    @Override
    public void debug(Marker marker, String msg, Throwable t) {
        debugMessages.add(new LogMessage(marker, msg, t));
        logger.debug(marker, msg, t);
    }

    
    
    // INFO
    @Override
    public boolean isInfoEnabled() {
        return logger.isInfoEnabled();
    }

    @Override
    public void info(String msg) {
        infoMessages.add(new LogMessage(null, msg, null));
        logger.info(msg);
    }

    @Override
    public void info(String format, Object arg) {
        infoMessages.add(new LogMessage(null, format, null, arg));
        logger.info(format, arg);
    }

    @Override
    public void info(String format, Object arg1, Object arg2) {
        infoMessages.add(new LogMessage(null, format, null, arg1, arg2));
        logger.info(format, arg1, arg2);
    }

    @Override
    public void info(String format, Object... arguments) {
        infoMessages.add(new LogMessage(null, format, null, arguments));
        logger.info(format, arguments);
    }

    @Override
    public void info(String msg, Throwable t) {
        infoMessages.add(new LogMessage(null, msg, t));
        logger.info(msg, t);
    }

    @Override
    public boolean isInfoEnabled(Marker marker) {
        return logger.isInfoEnabled(marker);
    }

    @Override
    public void info(Marker marker, String msg) {
        infoMessages.add(new LogMessage(marker, msg, null));
        logger.info(marker, msg);

    }

    @Override
    public void info(Marker marker, String format, Object arg) {
        infoMessages.add(new LogMessage(marker, format, null, arg));
        logger.info(marker, format, arg);

    }

    @Override
    public void info(Marker marker, String format, Object arg1, Object arg2) {
        infoMessages.add(new LogMessage(marker, format, null, arg1, arg2));
        logger.info(marker, format, arg1, arg2);
    }

    @Override
    public void info(Marker marker, String format, Object... argArray) {
        infoMessages.add(new LogMessage(marker, format, null, argArray));
        logger.info(marker, format, argArray);
    }

    @Override
    public void info(Marker marker, String msg, Throwable t) {
        infoMessages.add(new LogMessage(marker, msg, t));
        logger.debug(marker, msg, t);
    }
    // WARN
    @Override
    public boolean isWarnEnabled() {
        return logger.isWarnEnabled();
    }

    @Override
    public void warn(String msg) {
        warnMessages.add(new LogMessage(null, msg, null));
        logger.warn(msg);
    }

    @Override
    public void warn(String format, Object arg) {
        warnMessages.add(new LogMessage(null, format, null, arg));
        logger.warn(format, arg);
    }

    @Override
    public void warn(String format, Object arg1, Object arg2) {
        warnMessages.add(new LogMessage(null, format, null, arg1, arg2));
        logger.warn(format, arg1, arg2);
    }

    @Override
    public void warn(String format, Object... arguments) {
        warnMessages.add(new LogMessage(null, format, null, arguments));
        logger.warn(format, arguments);
    }

    @Override
    public void warn(String msg, Throwable t) {
        warnMessages.add(new LogMessage(null, msg, t));
        logger.warn(msg, t);
    }

    @Override
    public boolean isWarnEnabled(Marker marker) {
        return logger.isWarnEnabled(marker);
    }

    @Override
    public void warn(Marker marker, String msg) {
        warnMessages.add(new LogMessage(marker, msg, null));
        logger.warn(marker, msg);

    }

    @Override
    public void warn(Marker marker, String format, Object arg) {
        warnMessages.add(new LogMessage(marker, format, null, arg));
        logger.warn(marker, format, arg);

    }

    @Override
    public void warn(Marker marker, String format, Object arg1, Object arg2) {
        warnMessages.add(new LogMessage(marker, format, null, arg1, arg2));
        logger.warn(marker, format, arg1, arg2);
    }

    @Override
    public void warn(Marker marker, String format, Object... argArray) {
        warnMessages.add(new LogMessage(marker, format, null, argArray));
        logger.warn(marker, format, argArray);
    }

    @Override
    public void warn(Marker marker, String msg, Throwable t) {
        warnMessages.add(new LogMessage(marker, msg, t));
        logger.warn(marker, msg, t);
    }
    // ERROR
    @Override
    public boolean isErrorEnabled() {
        return logger.isErrorEnabled();
    }

    @Override
    public void error(String msg) {
        errorMessages.add(new LogMessage(null, msg, null));
        logger.error(msg);
    }

    @Override
    public void error(String format, Object arg) {
        errorMessages.add(new LogMessage(null, format, null, arg));
        logger.error(format, arg);
    }

    @Override
    public void error(String format, Object arg1, Object arg2) {
        errorMessages.add(new LogMessage(null, format, null, arg1, arg2));
        logger.error(format, arg1, arg2);
    }

    @Override
    public void error(String format, Object... arguments) {
        errorMessages.add(new LogMessage(null, format, null, arguments));
        logger.error(format, arguments);
    }

    @Override
    public void error(String msg, Throwable t) {
        errorMessages.add(new LogMessage(null, msg, t));
        logger.error(msg, t);
    }

    @Override
    public boolean isErrorEnabled(Marker marker) {
        return logger.isErrorEnabled(marker);
    }

    @Override
    public void error(Marker marker, String msg) {
        errorMessages.add(new LogMessage(marker, msg, null));
        logger.error(marker, msg);

    }

    @Override
    public void error(Marker marker, String format, Object arg) {
        errorMessages.add(new LogMessage(marker, format, null, arg));
        logger.error(marker, format, arg);

    }

    @Override
    public void error(Marker marker, String format, Object arg1, Object arg2) {
        errorMessages.add(new LogMessage(marker, format, null, arg1, arg2));
        logger.error(marker, format, arg1, arg2);
    }

    @Override
    public void error(Marker marker, String format, Object... argArray) {
        errorMessages.add(new LogMessage(marker, format, null, argArray));
        logger.error(marker, format, argArray);
    }

    @Override
    public void error(Marker marker, String msg, Throwable t) {
        errorMessages.add(new LogMessage(marker, msg, t));
        logger.error(marker, msg, t);
    }

}
