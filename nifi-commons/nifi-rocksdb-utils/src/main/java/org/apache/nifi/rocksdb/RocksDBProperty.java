package org.apache.nifi.rocksdb;

import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;

import java.util.concurrent.TimeUnit;

/**
 * Each property is defined by its name in the file and its default value
 */
public enum RocksDBProperty {
    //FlowFileRepo Configuration Parameters
    SYNC_WARNING_PERIOD("rocksdb.sync.warning.period", "30 seconds"),
    CLAIM_CLEANUP_PERIOD("rocksdb.claim.cleanup.period", "30 seconds"),
    DESERIALIZATION_THREADS("rocksdb.deserialization.threads", "16"),
    DESERIALIZATION_BUFFER_SIZE("rocksdb.deserialization.buffer.size", "1000"),
    SYNC_PERIOD("rocksdb.sync.period", "10 milliseconds"),
    ACCEPT_DATA_LOSS("rocksdb.accept.data.loss", "false"),
    ENABLE_STALL_STOP("rocksdb.enable.stall.stop", "false"),
    STALL_PERIOD("rocksdb.stall.period", "100 milliseconds"),
    STALL_FLOWFILE_COUNT("rocksdb.stall.flowfile.count", "800000"),
    STALL_HEAP_USAGE_PERCENT("rocksdb.stall.heap.usage.percent", "95%"),
    STOP_FLOWFILE_COUNT("rocksdb.stop.flowfile.count", "1100000"),
    STOP_HEAP_USAGE_PERCENT("rocksdb.stop.heap.usage.percent", "99.9%"),
    REMOVE_ORPHANED_FLOWFILES("rocksdb.remove.orphaned.flowfiles.on.startup", "false"),
    ENABLE_RECOVERY_MODE("rocksdb.enable.recovery.mode", "false"),
    RECOVERY_MODE_FLOWFILE_LIMIT("rocksdb.recovery.mode.flowfile.count", "5000"),

    //RocksDB Configuration Parameters
    DB_PARALLEL_THREADS("rocksdb.parallel.threads", "8"),
    MAX_WRITE_BUFFER_NUMBER("rocksdb.max.write.buffer.number", "4"),
    WRITE_BUFFER_SIZE("rocksdb.write.buffer.size", "256 MB"),
    LEVEL_O_SLOWDOWN_WRITES_TRIGGER("rocksdb.level.0.slowdown.writes.trigger", "20"),
    LEVEL_O_STOP_WRITES_TRIGGER("rocksdb.level.0.stop.writes.trigger", "40"),
    DELAYED_WRITE_RATE("rocksdb.delayed.write.bytes.per.second", "16 MB"),
    MAX_BACKGROUND_FLUSHES("rocksdb.max.background.flushes", "1"),
    MAX_BACKGROUND_COMPACTIONS("rocksdb.max.background.compactions", "1"),
    MIN_WRITE_BUFFER_NUMBER_TO_MERGE("rocksdb.min.write.buffer.number.to.merge", "1"),
    STAT_DUMP_PERIOD("rocksdb.stat.dump.period", "600 sec"),
    ;

    final String propertyName;
    final String defaultValue;

    RocksDBProperty(String propertyName, String defaultValue) {
        this.propertyName = propertyName;
        this.defaultValue = defaultValue;
    }

    /**
     * @param niFiProperties The Properties file
     * @param timeUnit       The desired time unit
     * @return The property Value in the desired units
     */
    public long getTimeValue(NiFiProperties niFiProperties, String propertyPrefix, TimeUnit timeUnit) {
        String propertyValue = niFiProperties.getProperty(propertyPrefix + this.propertyName, this.defaultValue);
        long timeValue = 0L;
        try {
            timeValue = Math.round(FormatUtils.getPreciseTimeDuration(propertyValue, timeUnit));
        } catch (IllegalArgumentException e) {
            this.generateIllegalArgumentException(propertyPrefix, propertyValue, e);
        }
        return timeValue;
    }

    /**
     * @param niFiProperties The Properties file
     * @return The property value as a boolean
     */
    public boolean getBooleanValue(NiFiProperties niFiProperties, String propertyPrefix) {
        String propertyValue = niFiProperties.getProperty(propertyPrefix + this.propertyName, this.defaultValue);
        return Boolean.parseBoolean(propertyValue);
    }

    /**
     * @param niFiProperties The Properties file
     * @return The property value as an int
     */
    public int getIntValue(NiFiProperties niFiProperties, String propertyPrefix) {
        String propertyValue = niFiProperties.getProperty(propertyPrefix + this.propertyName, this.defaultValue);
        int returnValue = 0;
        try {
            returnValue = Integer.parseInt(propertyValue);
        } catch (NumberFormatException e) {
            this.generateIllegalArgumentException(propertyPrefix, propertyValue, e);
        }
        return returnValue;
    }

    /**
     * @param niFiProperties The Properties file
     * @return The property value as a number of bytes
     */
    public long getByteCountValue(NiFiProperties niFiProperties, String propertyPrefix) {
        long returnValue = 0L;
        String propertyValue = niFiProperties.getProperty(propertyPrefix + this.propertyName, this.defaultValue);
        try {
            double writeBufferDouble = DataUnit.parseDataSize(propertyValue, DataUnit.B);
            returnValue = (long) (writeBufferDouble < Long.MAX_VALUE ? writeBufferDouble : Long.MAX_VALUE);
        } catch (IllegalArgumentException e) {
            this.generateIllegalArgumentException(propertyPrefix, propertyValue, e);
        }
        return returnValue;
    }

    /**
     * @param niFiProperties The Properties file
     * @return The property value as a percent
     */
    public double getPercentValue(NiFiProperties niFiProperties, String propertyPrefix) {
        String propertyValue = niFiProperties.getProperty(propertyPrefix + this.propertyName, this.defaultValue).replace('%', ' ');
        double returnValue = 0.0D;
        try {
            returnValue = Double.parseDouble(propertyValue) / 100D;
            if (returnValue > 1.0D) {
                this.generateIllegalArgumentException(propertyPrefix, propertyValue, null);
            }
        } catch (NumberFormatException e) {
            this.generateIllegalArgumentException(propertyPrefix, propertyValue, e);
        }
        return returnValue;
    }

    /**
     * @param niFiProperties The Properties file
     * @return The property value as a long
     */
    public long getLongValue(NiFiProperties niFiProperties, String propertyPrefix) {
        String propertyValue = niFiProperties.getProperty(propertyPrefix + this.propertyName, this.defaultValue);
        long returnValue = 0L;
        try {
            returnValue = Long.parseLong(propertyValue);
        } catch (NumberFormatException e) {
            this.generateIllegalArgumentException(propertyPrefix, propertyValue, e);
        }
        return returnValue;
    }

    void generateIllegalArgumentException(String propertyPrefix, String badValue, Throwable t) {
        throw new IllegalArgumentException("The NiFi Property: [" + propertyPrefix + this.propertyName + "] with value: [" + badValue + "] is not valid", t);
    }

    public String getPropertyName(String propertyPrefix) {
        return propertyPrefix + propertyName;
    }
}
