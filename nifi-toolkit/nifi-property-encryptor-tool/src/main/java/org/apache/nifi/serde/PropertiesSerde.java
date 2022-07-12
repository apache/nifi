package org.apache.nifi.serde;

public abstract class PropertiesSerde {

//
////    private static PropertiesLoader getNiFiPropertiesLoader(final String keyHex) {
////        keyHex == null ? new NiFiPropertiesLoader() : NiFiPropertiesLoader.withKey(keyHex)
////    }
//    public static PropertiesLoader getPropertiesLoader() {
//        return getPropertiesLoader(null);
//    }
//
//    public static PropertiesLoader getPropertiesLoader(final String keyHex) {
//
//    }
//
//    //BufferedReader reader = new BufferedReader(new FileInputStreamReader(pathToFile));
//
//    /**
//     * Loads the {@link NiFiProperties} instance from the provided file path (restoring the original value of the System property {@code nifi.properties.file.path} after loading this instance).
//     *
//     * @return the NiFiProperties instance
//     * @throw IOException if the nifi.properties file cannot be read
//     */
//    private NiFiProperties loadNiFiProperties(String keyHex) throws IOException {
//        File niFiPropertiesFile = null;
//        if (niFiPropertiesPath && (niFiPropertiesFile = new File(niFiPropertiesPath)).exists()) {
//            NiFiProperties properties
//            try {
//                properties = getNiFiPropertiesLoader(keyHex).load(niFiPropertiesFile)
//                logger.info("Loaded NiFiProperties instance with ${properties.size()} properties")
//                return properties
//            } catch (RuntimeException e) {
//                if (isVerbose) {
//                    logger.error("Encountered an error", e)
//                }
//                throw new IOException("Cannot load NiFiProperties from [${niFiPropertiesPath}]", e)
//            }
//        } else {
//            printUsageAndThrow("Cannot load NiFiProperties from [${niFiPropertiesPath}]", ExitCode.ERROR_READING_NIFI_PROPERTIES)
//        }
//    }
}
