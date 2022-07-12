package org.apache.nifi.properties;

public abstract class AbstractPropertiesLoader {

     String keyHex;

    /**
     * Sets the hexadecimal key used to unprotect properties encrypted with a
     * {@link SensitivePropertyProvider}. If the key has already been set,
     * calling this method will throw a {@link RuntimeException}.
     *
     * @param keyHex the key in hexadecimal format
     */
    public void setKeyHex(final String keyHex) {
        if (this.keyHex == null || this.keyHex.trim().isEmpty()) {
            this.keyHex = keyHex;
        } else {
            throw new RuntimeException("Cannot overwrite an existing key");
        }
    }
}
