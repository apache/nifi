/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.rwfile;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.*;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import java.io.*;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author every
 */
@Tags({"append", "local", "copy", "archive", "files", "filesystem"})
@CapabilityDescription("Writes the contents of a FlowFile to the local file system")
public class StandardWriteFileService extends AbstractControllerService implements WriteFileService {

    public static final String APPEND_RESOLUTION = "append";
    public static final String REPLACE_RESOLUTION = "replace";
    public static final String IGNORE_RESOLUTION = "ignore";
    public static final String FAIL_RESOLUTION = "fail";

    public static final Pattern RWX_PATTERN = Pattern.compile("^([r-][w-])([x-])([r-][w-])([x-])([r-][w-])([x-])$");
    public static final Pattern NUM_PATTERN = Pattern.compile("^[0-7]{3}$");

    private static final Validator PERMISSIONS_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            ValidationResult.Builder vr = new ValidationResult.Builder();
            if (context.isExpressionLanguagePresent(input)) {
                return new ValidationResult.Builder().subject(subject).input(input).explanation("Expression Language Present").valid(true).build();
            }

            if (RWX_PATTERN.matcher(input).matches() || NUM_PATTERN.matcher(input).matches()) {
                return vr.valid(true).build();
            }
            return vr.valid(false)
                    .subject(subject)
                    .input(input)
                    .explanation("This must be expressed in rwxr-x--- form or octal triplet form.")
                    .build();
        }
    };
    public static final PropertyDescriptor CONFLICT_RESOLUTION = new PropertyDescriptor.Builder()
            .name("Conflict Resolution Strategy")
            .description("Indicates what should happen when a file with the same name already exists in the output directory")
            .required(true)
            .defaultValue(FAIL_RESOLUTION)
            .allowableValues(APPEND_RESOLUTION,REPLACE_RESOLUTION, IGNORE_RESOLUTION, FAIL_RESOLUTION)
            .build();

    static final PropertyDescriptor PERM_DENIED_LOG_LEVEL = new PropertyDescriptor.Builder()
            .name("Log level when permission denied")
            .description("Log level to use in case user " + System.getProperty("user.name") + " does not have sufficient permissions to read the file")
            .allowableValues(LogLevel.values())
            .defaultValue(LogLevel.ERROR.toString())
            .required(true)
            .build();

    public static final PropertyDescriptor CREATE_DIRS = new PropertyDescriptor.Builder()
            .name("Create Missing Directories")
            .description("If true, then missing destination directories will be created. If false, flowfiles are penalized and sent to failure.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor CHANGE_PERMISSIONS = new PropertyDescriptor.Builder()
            .name("Permissions")
            .description("Sets the permissions on the output file to the value of this attribute.  Format must be either UNIX rwxrwxrwx with a - in "
                    + "place of denied permissions (e.g. rw-r--r--) or an octal number (e.g. 644).  You may also use expression language such as "
                    + "${file.permissions}.")
            .required(false)
            .addValidator(PERMISSIONS_VALIDATOR)
            .build();
    public static final PropertyDescriptor CHANGE_OWNER = new PropertyDescriptor.Builder()
            .name("Owner")
            .description("Sets the owner on the output file to the value of this attribute.  You may also use expression language such as "
                    + "${file.owner}. Note on many operating systems Nifi must be running as a super-user to have the permissions to set the file owner.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor CHANGE_GROUP = new PropertyDescriptor.Builder()
            .name("Group")
            .description("Sets the group on the output file to the value of this attribute.  You may also use expression language such "
                    + "as ${file.group}.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static Map<String, Map> catchfile = null;
    private static List<PropertyDescriptor> properties;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(CONFLICT_RESOLUTION);
        props.add(PERM_DENIED_LOG_LEVEL);
        props.add(CREATE_DIRS);
        props.add(CHANGE_PERMISSIONS);
        props.add(CHANGE_OWNER);
        props.add(CHANGE_GROUP);
        properties = Collections.unmodifiableList(props);
        catchfile = new ConcurrentHashMap();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }
    String conflictResponse;
    LogLevel levelPermDenied;
    boolean create_dirs;
    String permissions;
    String owner;
    String group;
    /**
     * @param context
     *            the configuration context
     * @throws InitializationException
     *             if unable to create a database connection
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        conflictResponse = context.getProperty(CONFLICT_RESOLUTION).getValue();
        levelPermDenied = LogLevel.valueOf(context.getProperty(PERM_DENIED_LOG_LEVEL).getValue());
        create_dirs = context.getProperty(CREATE_DIRS).asBoolean();

        permissions = context.getProperty(CHANGE_PERMISSIONS).getValue();
        owner = context.getProperty(CHANGE_OWNER).getValue();
        group = context.getProperty(CHANGE_GROUP).getValue();
    }

    @OnDisabled
    public void shutdown() {
        catchfile.clear();
    }

    @Override
    public void execute() throws ProcessException {

    }

    @Override
    public Map writeFile(String filepath, String inputStream, String charSet) {
        Map rtn = new HashMap();

        long start = System.currentTimeMillis();
        getLogger().debug("catchfile:"+catchfile);
        try{
            BufferedWriter bw = null;
            Map file_info = catchfile.get(filepath);
            if(file_info == null||file_info.isEmpty()){

                synchronized (catchfile) {
                    file_info = catchfile.get(filepath);
                    if(file_info==null) {
                        File file = new File(filepath);
                        // Verify read permission on file
                        Path rootDirPath = file.toPath().getParent();
                        if (!Files.exists(rootDirPath)) {
                            if (create_dirs) {
                                Path existing = rootDirPath;
                                while (!Files.exists(existing)) {
                                    existing = existing.getParent();
                                }
                                if (permissions != null && !permissions.trim().isEmpty()) {
                                    try {
                                        String perms = stringPermissions(permissions, true);
                                        if (!perms.isEmpty()) {
                                            Files.createDirectories(rootDirPath, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString(perms)));
                                        } else {
                                            Files.createDirectories(rootDirPath);
                                        }
                                    } catch (Exception e) {
                                        getLogger().error("Could not set create directory with permissions {} because {}", new Object[]{permissions, e});
                                    }
                                } else {
                                    Files.createDirectories(rootDirPath);
                                }

                                boolean chOwner = owner != null && !owner.trim().isEmpty();
                                boolean chGroup = group != null && !group.trim().isEmpty();
                                if (chOwner || chGroup) {
                                    Path currentPath = rootDirPath;
                                    while (!currentPath.equals(existing)) {
                                        if (chOwner) {
                                            try {
                                                UserPrincipalLookupService lookupService = currentPath.getFileSystem().getUserPrincipalLookupService();
                                                Files.setOwner(currentPath, lookupService.lookupPrincipalByName(owner));
                                            } catch (Exception e) {
                                                getLogger().warn("Could not set directory owner to {} because {}", new Object[]{owner, e});
                                                rtn.put("over", false);
                                                rtn.put("msg", "Could not set directory owner");
                                                return rtn;
                                            }
                                        }
                                        if (chGroup) {
                                            try {
                                                UserPrincipalLookupService lookupService = currentPath.getFileSystem().getUserPrincipalLookupService();
                                                PosixFileAttributeView view = Files.getFileAttributeView(currentPath, PosixFileAttributeView.class);
                                                view.setGroup(lookupService.lookupPrincipalByGroupName(group));
                                            } catch (Exception e) {
                                                getLogger().warn("Could not set file group to {} because {}", new Object[]{group, e});
                                                rtn.put("over", false);
                                                rtn.put("msg", "Could not set file group");
                                                return rtn;
                                            }
                                        }
                                        currentPath = currentPath.getParent();
                                    }
                                }
                            } else {
                                getLogger().error("Penalizing and routing to 'failure' because the output directory {} does not exist and Processor is "
                                        + "configured not to create missing directories", new Object[]{rootDirPath});
                                rtn.put("over", false);
                                rtn.put("msg", "not to create missing directories");
                                return rtn;
                            }
                        }

                        if (file.exists()) {
                            switch (conflictResponse) {
                                case APPEND_RESOLUTION:
                                    break;
                                case REPLACE_RESOLUTION:
                                    file.delete();
                                    getLogger().info("Deleted {} as configured in order to replace with the contents ", new Object[]{filepath});
                                    break;
                                case IGNORE_RESOLUTION:
                                    getLogger().info("Transferring {} to success because file with same name already exists", new Object[]{filepath});
                                    rtn.put("over", true);
                                    return rtn;
                                case FAIL_RESOLUTION:
                                    getLogger().warn("Penalizing {} and routing to failure as configured because file with the same name already exists", new Object[]{filepath});
                                    rtn.put("over", false);
                                    rtn.put("msg", "file already exists");
                                    return rtn;
                                default:
                                    break;
                            }
                        }

                        FileOutputStream fos = new FileOutputStream(file,true);
                        OutputStreamWriter osw = new OutputStreamWriter(fos, charSet);
                        bw = new BufferedWriter(osw);
                        file_info = new HashMap();
                        file_info.put("bw", bw);
                        file_info.put("osw", osw);
                        file_info.put("fos", fos);
                        file_info.put("writeCount", 0L);
                        catchfile.put(filepath, file_info);
                    }else{
                        bw = (BufferedWriter) file_info.get("bw");
                    }
                }
            }else {
                bw = (BufferedWriter) file_info.get("bw");
            }
            long end = System.currentTimeMillis();
            getLogger().debug("write data to file 1："+(end-start)+" " +start +" "+ ++rows);

            synchronized (bw) {
                bw.write(inputStream);
                long writeCount = (long)file_info.get("writeCount")+1;
                file_info.put("writeCount",writeCount);
                rtn.put("writeCount",writeCount);
            }

            getLogger().debug("write data to file 2："+(System.currentTimeMillis()-end)+" " +end + inputStream);
            rtn.put("over", true);
        } catch (IOException ioe) {
            rtn.put("over",false);
            rtn.put("msg",ioe.toString());
            getLogger().error("Could not fetch file from file system for {} ", new Object[]{ ioe.toString()}, ioe);
        } catch (Throwable ioe) {
            rtn.put("over",false);
            rtn.put("msg",ioe.toString());
            getLogger().error("msg:{}",new Object[]{ioe.getMessage()},ioe);
        }
        return rtn;
    }

    @Override
    public Map closeFile(String filepath) {
        Map rtn = new HashMap();
        Map file_info ;
        synchronized (catchfile) {
            file_info = catchfile.remove(filepath);
        }
        if(file_info == null||file_info.isEmpty()){

        }else{
            FileOutputStream fos = (FileOutputStream) file_info.remove("fos");
            OutputStreamWriter osw = (OutputStreamWriter) file_info.remove("osw");
            BufferedWriter bw = (BufferedWriter) file_info.remove("bw");
            int loop = 10;
            while(bw!=null){
                try {
                    bw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                }
                if(--loop == 0){
                    break;
                }
            }
            loop = 10;
            while(osw!=null){
                try {
                    osw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                }
                if(--loop == 0){
                    break;
                }
            }
            loop = 10;
            while(fos!=null){
                try {
                    fos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                }
                if(--loop == 0){
                    break;
                }
            }

        }
        rtn.put("over", true);
        return rtn;
    }

    int rows = 0;

    protected String stringPermissions(String perms, boolean directory) {
        String permissions = "";
        Matcher rwx = RWX_PATTERN.matcher(perms);
        if (rwx.matches()) {
            if (directory) {
                // To read or write, directory access will be required
                StringBuilder permBuilder = new StringBuilder();
                permBuilder.append("$1");
                permBuilder.append(rwx.group(1).equals("--") ? "$2" : "x");
                permBuilder.append("$3");
                permBuilder.append(rwx.group(3).equals("--") ? "$4" : "x");
                permBuilder.append("$5");
                permBuilder.append(rwx.group(5).equals("--") ? "$6" : "x");
                permissions = rwx.replaceAll(permBuilder.toString());
            } else {
                permissions = perms;
            }
        } else if (NUM_PATTERN.matcher(perms).matches()) {
            try {
                int number = Integer.parseInt(perms, 8);
                StringBuilder permBuilder = new StringBuilder();
                if ((number & 0x100) > 0) {
                    permBuilder.append('r');
                } else {
                    permBuilder.append('-');
                }
                if ((number & 0x80) > 0) {
                    permBuilder.append('w');
                } else {
                    permBuilder.append('-');
                }
                if (directory || (number & 0x40) > 0) {
                    permBuilder.append('x');
                } else {
                    permBuilder.append('-');
                }
                if ((number & 0x20) > 0) {
                    permBuilder.append('r');
                } else {
                    permBuilder.append('-');
                }
                if ((number & 0x10) > 0) {
                    permBuilder.append('w');
                } else {
                    permBuilder.append('-');
                }
                if ((number & 0x8) > 0) {
                    permBuilder.append('x');
                } else {
                    if (directory && (number & 0x30) > 0) {
                        // To read or write, directory access will be required
                        permBuilder.append('x');
                    } else {
                        permBuilder.append('-');
                    }
                }
                if ((number & 0x4) > 0) {
                    permBuilder.append('r');
                } else {
                    permBuilder.append('-');
                }
                if ((number & 0x2) > 0) {
                    permBuilder.append('w');
                } else {
                    permBuilder.append('-');
                }
                if ((number & 0x1) > 0) {
                    permBuilder.append('x');
                } else {
                    if (directory && (number & 0x6) > 0) {
                        // To read or write, directory access will be required
                        permBuilder.append('x');
                    } else {
                        permBuilder.append('-');
                    }
                }
                permissions = permBuilder.toString();
            } catch (NumberFormatException ignore) {
            }
        }

        return permissions;
    }
}
