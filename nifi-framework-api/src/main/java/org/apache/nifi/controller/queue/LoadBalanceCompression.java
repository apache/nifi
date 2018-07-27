package org.apache.nifi.controller.queue;

public enum LoadBalanceCompression {
    /**
     * FlowFiles will not be compressed
     */
    DO_NOT_COMPRESS,

    /**
     * FlowFiles' attributes will be compressed, but the FlowFiles' contents will not be
     */
    COMPRESS_ATTRIBUTES_ONLY,

    /**
     * FlowFiles' attributes and content will be compressed
     */
    COMPRESS_ATTRIBUTES_AND_CONTENT;
}
