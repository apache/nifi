package org.apache.nifi.processors.groovyx.util;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Helpers to work with files
 */
public class Files {

    /**
     * Classpath list separated by semicolon. You can use masks like `*`, `*.jar` in file name.
     * Please avoid using this parameter because of deploy complexity :)
     *
     * @param classpath
     * @return file list defined by classpath parameter
     */
    public static Set<File> listPathsFiles(String classpath) {
        if (classpath == null || classpath.length() == 0) {
            return Collections.emptySet();
        }
        Set<File> files = new HashSet<>();
        if (classpath != null && classpath.length() > 0) {
            for (String cp : classpath.split("\\s*;\\s*")) {
                files.addAll(listPathFiles(cp));
            }
        }
        return files;
    }

    /**
     * returns file list from one path. the path coud be exact filename (one file returned), exact directory (all files from dir returned)
     * or exact dir with masked file names like ./dir/*.jar (all jars returned)
     */
    public static List<File> listPathFiles(String path) {
        File f = new File(path);
        String fname = f.getName();
        if (fname != null && (fname.indexOf("?") >= 0 || fname.indexOf("*") >= 0)) {
            Pattern pattern = Pattern.compile(fname.replace(".", "\\.").replace("?", ".?").replace("*", ".*?"));
            File[] list = f.getParentFile().listFiles((FilenameFilter) (dir, name) -> pattern.matcher(name).find());
            return Arrays.asList(list);
        }
        if (!f.exists()) {
            System.err.println("WARN: path not found for: " + f);
        }
        return Arrays.asList(f);
    }
}
