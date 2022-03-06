package org.apache.nifi.processors.ngsi.ngsi.utils;

import javax.xml.bind.DatatypeConverter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public final class CommonConstants {

    // Maximum values
    public static final int MAX_CONNS           = 500;
    public static final int MAX_CONNS_PER_ROUTE = 100;

    // encoding
    public static final String INTERNAL_CONCATENATOR = "=";
    public static final String CONCATENATOR = "xffff";
    public static final String OLD_CONCATENATOR = "_";

    /**
     * Constructor. It is private since utility classes should not have a public or default constructor.
     */
    private CommonConstants() {
    } // CommonConstants

    /**
     * Gets the milliseconds version of the given timestamp.
     * @param timestamp
     * @return The milliseconds version of the given timestamp
     * @throws java.text.ParseException
     */
    public static long getMilliseconds(String timestamp) throws java.text.ParseException {
        return DatatypeConverter.parseDateTime(timestamp).getTime().getTime();
    } // getMilliseconds

    /**
     * Gets the human redable version of timestamp expressed in miliseconds.
     * @param ts
     * @param addUTC
     * @return
     */
    public static String getHumanReadable(long ts, boolean addUTC) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        String humanRedable = sdf.format(new Date(ts));
        humanRedable += (addUTC ? "T" : " ");
        sdf = new SimpleDateFormat("HH:mm:ss.S");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        humanRedable += sdf.format(new Date(ts)) + (addUTC ? "Z" : "");
        return humanRedable;
    } // getHumanRedable

} // CommonConstants
