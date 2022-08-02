package org.apache.iotdb.processors.model;

import java.util.HashMap;
import java.util.Set;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

public class Field {
    private String tsName;
    private TSDataType dataType;
    private TSEncoding encoding;
    private CompressionType compressionType;

    private static final HashMap<String, TSDataType> typeMap =
            new HashMap<String, TSDataType>() {
                {
                    put("INT32", TSDataType.INT32);
                    put("INT64", TSDataType.INT64);
                    put("FLOAT", TSDataType.FLOAT);
                    put("DOUBLE", TSDataType.DOUBLE);
                    put("BOOLEAN", TSDataType.BOOLEAN);
                    put("TEXT", TSDataType.TEXT);
                }
            };

    private static final HashMap<String, TSEncoding> encodingMap =
            new HashMap<String, TSEncoding>() {
                {
                    put("PLAIN", TSEncoding.PLAIN);
                    put("DICTIONARY", TSEncoding.DICTIONARY);
                    put("RLE", TSEncoding.RLE);
                    put("DIFF", TSEncoding.DIFF);
                    put("TS_2DIFF", TSEncoding.TS_2DIFF);
                    put("BITMAP", TSEncoding.BITMAP);
                    put("GORILLA_V1", TSEncoding.GORILLA_V1);
                    put("REGULAR", TSEncoding.REGULAR);
                    put("GORILLA", TSEncoding.GORILLA);
                }
            };

    private static final HashMap<String, CompressionType> compressionMap =
            new HashMap<String, CompressionType>() {
                {
                    put("UNCOMPRESSED", CompressionType.UNCOMPRESSED);
                    put("SNAPPY", CompressionType.SNAPPY);
                    put("GZIP", CompressionType.GZIP);
                    put("LZO", CompressionType.LZO);
                    put("SDT", CompressionType.SDT);
                    put("PAA", CompressionType.PAA);
                    put("PLA", CompressionType.PLA);
                    put("LZ4", CompressionType.LZ4);
                }
            };

    public Field(String tsName, String dataType, String encoding, String compressionType) {
        this.tsName = tsName;
        this.dataType = typeMap.get(dataType);
        this.encoding = encodingMap.get(encoding);
        this.compressionType = compressionMap.get(compressionType);
    }

    public Field(String tsName, TSDataType dataType) {
        this.tsName = tsName;
        this.dataType = dataType;
    }

    public String getTsName() {
        return tsName;
    }

    public void setTsName(String tsName) {
        this.tsName = tsName;
    }

    public TSDataType getDataType() {
        return dataType;
    }

    public void setDataType(TSDataType dataType) {
        this.dataType = dataType;
    }

    public TSEncoding getEncoding() {
        return encoding;
    }

    public void setEncoding(TSEncoding encoding) {
        this.encoding = encoding;
    }

    public CompressionType getCompressionType() {
        return compressionType;
    }

    public void setCompressionType(CompressionType compressionType) {
        this.compressionType = compressionType;
    }

    public static Set<String> getSupportedDataType() {
        return typeMap.keySet();
    }

    public static Set<String> getSupportedEncoding() {
        return encodingMap.keySet();
    }

    public static Set<String> getSupportedCompressionType() {
        return compressionMap.keySet();
    }
}
