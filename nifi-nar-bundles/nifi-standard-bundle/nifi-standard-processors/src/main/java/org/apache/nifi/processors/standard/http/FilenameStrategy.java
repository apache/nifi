package org.apache.nifi.processors.standard.http;

public enum FilenameStrategy {
    RANDOM("FlowFile's filename attribute will be a random value."),
    URL_PATH("FlowFile's filename attribute will be extracted from the remote URL's path. "
            + "If the path doesn't exist, the filename will be a random value.");

    private final String description;

    FilenameStrategy(final String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }
}
