This directory is used to mimic NiFi's own home directory: the JVM hosting the
TestNiFiInstance has to be started here. Once started, TestNiFiInstance then
creates symlinks to the actual NiFi installation directory.