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
package org.apache.nifi.processors.standard;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileOwnerAttributeView;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

@TriggerWhenEmpty
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"local", "files", "filesystem", "ingest", "ingress", "get", "source", "input"})
@CapabilityDescription("Creates FlowFiles from files in a directory.  NiFi will ignore files it doesn't have at least read permissions for.")
@WritesAttributes({
    @WritesAttribute(attribute = "filename", description = "The filename is set to the name of the file on disk"),
    @WritesAttribute(attribute = "path", description = "The path is set to the relative path of the file's directory on disk. For example, "
            + "if the <Input Directory> property is set to /tmp, files picked up from /tmp will have the path attribute set to ./. If "
            + "the <Recurse Subdirectories> property is set to true and a file is picked up from /tmp/abc/1/2/3, then the path attribute will "
            + "be set to abc/1/2/3"),
    @WritesAttribute(attribute = "file.creationTime", description = "The date and time that the file was created. May not work on all file systems"),
    @WritesAttribute(attribute = "file.lastModifiedTime", description = "The date and time that the file was last modified. May not work on all "
            + "file systems"),
    @WritesAttribute(attribute = "file.lastAccessTime", description = "The date and time that the file was last accessed. May not work on all "
            + "file systems"),
    @WritesAttribute(attribute = "file.owner", description = "The owner of the file. May not work on all file systems"),
    @WritesAttribute(attribute = "file.group", description = "The group owner of the file. May not work on all file systems"),
    @WritesAttribute(attribute = "file.permissions", description = "The read/write/execute permissions of the file. May not work on all file systems"),
    @WritesAttribute(attribute = "absolute.path", description = "The full/absolute path from where a file was picked up. The current 'path' "
            + "attribute is still populated, but may be a relative path")})
@SeeAlso({PutFile.class, FetchFile.class})
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.READ_FILESYSTEM,
                        explanation = "Provides operator the ability to read from any file that NiFi has access to."),
                @Restriction(
                        requiredPermission = RequiredPermission.WRITE_FILESYSTEM,
                        explanation = "Provides operator the ability to delete any file that NiFi has access to.")
        }
)
public class GetFile extends AbstractProcessor {

    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
            .displayName("Input Directory")
            .name("Input Directory")
            .description("The input directory from which to pull files")
            .required(true)
            .addValidator(StandardValidators.createDirectoryExistsValidator(true, false))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    public static final PropertyDescriptor RECURSE = new PropertyDescriptor.Builder()
            .name("Recurse Subdirectories")
            .description("Indicates whether or not to pull files from subdirectories")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();
    public static final PropertyDescriptor KEEP_SOURCE_FILE = new PropertyDescriptor.Builder()
            .name("Keep Source File")
            .displayName("Keep Source File")
            .description("If true, the file is not deleted after it has been copied to the Content Repository; "
                    + "this causes the file to be picked up continually and is useful for testing purposes.  "
                    + "If not keeping original NiFi will need write permissions on the directory it is pulling "
                    + "from otherwise it will ignore the file.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor FILE_FILTER = new PropertyDescriptor.Builder()
            .displayName("File Filter")
            .name("File Filter")
            .description("Only files whose names match the given regular expression will be picked up")
            .required(true)
            .defaultValue("[^\\.].*")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();
    public static final PropertyDescriptor PATH_FILTER = new PropertyDescriptor.Builder()
            .displayName("Path Filter")
            .name("Path Filter")
            .description("When " + RECURSE.getName() + " is true, then only subdirectories whose path matches the given regular expression will be scanned")
            .required(false)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();
    public static final PropertyDescriptor HIDDEN_DIRECTORIES= new PropertyDescriptor.Builder()
            .displayName("Process Hidden Directories")
            .name("Process Hidden Directories")
            .description("When " + RECURSE.getName() + " is true, then only subdirectories whose path matches the given regular expression will be scanned")
            .required(true)
            .allowableValues("true","false")
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    public static final PropertyDescriptor MIN_AGE = new PropertyDescriptor.Builder()
            .displayName("Minimum File Age")
            .name("Minimum File Age")
            .description("The minimum age that a file must be in order to be pulled; any file younger than this amount of time (according to last modification date) will be ignored")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("0 sec")
            .build();
    public static final PropertyDescriptor MAX_AGE = new PropertyDescriptor.Builder()
            .displayName("Maximum File Age")
            .name("Maximum File Age")
            .description("The maximum age that a file must be in order to be pulled; any file older than this amount of time (according to last modification date) will be ignored")
            .required(false)
            .addValidator(StandardValidators.createTimePeriodValidator(100, TimeUnit.MILLISECONDS, Long.MAX_VALUE, TimeUnit.NANOSECONDS))
            .build();
    public static final PropertyDescriptor MIN_SIZE = new PropertyDescriptor.Builder()
            .displayName("Minimum File Size")
            .name("Minimum File Size")
            .description("The minimum size that a file must be in order to be pulled")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("0 B")
            .build();
    public static final PropertyDescriptor MAX_SIZE = new PropertyDescriptor.Builder()
            .displayName("Maximum File Size")
            .name("Maximum File Size")
            .description("The maximum size that a file can be in order to be pulled")
            .required(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();
    public static final PropertyDescriptor IGNORE_HIDDEN_FILES = new PropertyDescriptor.Builder()
            .displayName("Ignore Hidden Files")
            .name("Ignore Hidden Files")
            .description("Indicates whether or not hidden files should be ignored")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();
    public static final PropertyDescriptor POLLING_INTERVAL = new PropertyDescriptor.Builder()
            .displayName("Polling Interval")
            .name("Polling Interval")
            .description("Indicates how long to wait before performing a directory listing")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("0 sec")
            .build();
    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .displayName("Batch Size")
            .name("Batch Size")
            .description("The maximum number of files to pull in each iteration")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10")
            .build();
    public static final PropertyDescriptor LISTING_SIZE = new PropertyDescriptor.Builder()
            .displayName("Listing Size")
            .name("Listing Size")
            .description("Minimum file queue size to stop file listing fetch.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .build();

    public static final String FILE_CREATION_TIME_ATTRIBUTE = "file.creationTime";
    public static final String FILE_LAST_MODIFY_TIME_ATTRIBUTE = "file.lastModifiedTime";
    public static final String FILE_LAST_ACCESS_TIME_ATTRIBUTE = "file.lastAccessTime";
    public static final String FILE_OWNER_ATTRIBUTE = "file.owner";
    public static final String FILE_GROUP_ATTRIBUTE = "file.group";
    public static final String FILE_PERMISSIONS_ATTRIBUTE = "file.permissions";
    public static final String FILE_MODIFY_DATE_ATTR_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("All files are routed to success").build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    private final AtomicReference<FileFilter> fileFilterRef = new AtomicReference<>();

    private final BlockingQueue<File> fileQueue = new LinkedBlockingQueue<>();
    private final Set<File> inProcess = new HashSet<>();    // guarded by queueLock
    private final Set<File> recentlyProcessed = new HashSet<>();    // guarded by queueLock
    private final Lock queueLock = new ReentrantLock();

    private final Lock listingLock = new ReentrantLock();

    private final AtomicLong queueLastUpdated = new AtomicLong(0L);
    private final AtomicBoolean notFinished = new AtomicBoolean(false);
    private final BlockingQueue<File> fileListingBlockingQueue = new LinkedBlockingQueue<>();
    private final AtomicReference<String> eHandle = new AtomicReference<>();
    private final Lock processLock = new ReentrantLock();
    private final ConcurrentLinkedQueue<File> tmpFileList = new ConcurrentLinkedQueue<>();

    private final AtomicBoolean  batchSizeNotMet = new AtomicBoolean(true);
    private final AtomicBoolean processingList = new AtomicBoolean(false);
    private final AtomicBoolean listSizeNotMet=new AtomicBoolean(true);
    private final AtomicBoolean processHiddenDirectories = new AtomicBoolean();



    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DIRECTORY);
        properties.add(FILE_FILTER);
        properties.add(PATH_FILTER);
        properties.add(HIDDEN_DIRECTORIES);
        properties.add(LISTING_SIZE);
        properties.add(BATCH_SIZE);
        properties.add(KEEP_SOURCE_FILE);
        properties.add(RECURSE);
        properties.add(POLLING_INTERVAL);
        properties.add(IGNORE_HIDDEN_FILES);
        properties.add(MIN_AGE);
        properties.add(MAX_AGE);
        properties.add(MIN_SIZE);
        properties.add(MAX_SIZE);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        fileFilterRef.set(createFileFilter(context));
        fileQueue.clear();
    }

    private FileFilter createFileFilter(final ProcessContext context) {
        final long minSize = context.getProperty(MIN_SIZE).asDataSize(DataUnit.B).longValue();
        final Double maxSize = context.getProperty(MAX_SIZE).asDataSize(DataUnit.B);
        final long minAge = context.getProperty(MIN_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        final Long maxAge = context.getProperty(MAX_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        final boolean ignoreHidden = context.getProperty(IGNORE_HIDDEN_FILES).asBoolean();
        final Pattern filePattern = Pattern.compile(context.getProperty(FILE_FILTER).getValue());
        final String indir = context.getProperty(DIRECTORY).evaluateAttributeExpressions().getValue();
        final boolean recurseDirs = context.getProperty(RECURSE).asBoolean();
        final String pathPatternStr = context.getProperty(PATH_FILTER).getValue();
        final Pattern pathPattern = (!recurseDirs || pathPatternStr == null) ? null : Pattern.compile(pathPatternStr);
        final boolean keepOriginal = context.getProperty(KEEP_SOURCE_FILE).asBoolean();

        return file -> {
            if (minSize > file.length()) {
                return false;
            }
            if (maxSize != null && maxSize < file.length()) {
                return false;
            }
            final long fileAge = System.currentTimeMillis() - file.lastModified();
            if (minAge > fileAge) {
                return false;
            }
            if (maxAge != null && maxAge < fileAge) {
                return false;
            }
            if (ignoreHidden && file.isHidden()) {
                return false;
            }
            if (pathPattern != null) {
                Path reldir = Paths.get(indir).relativize(file.toPath()).getParent();
                if (reldir != null && !reldir.toString().isEmpty()) {
                    if (!pathPattern.matcher(reldir.toString()).matches()) {
                        return false;
                    }
                }
            }
            //Verify that we have at least read permissions on the file we're considering grabbing
            if (!Files.isReadable(file.toPath())) {
                return false;
            }

            //Verify that if we're not keeping original that we have write permissions on the directory the file is in
            if (!keepOriginal && !Files.isWritable(file.toPath().getParent())) {
                return false;
            }
            return filePattern.matcher(file.getName()).matches();
        };
    }

    private void getFiles(final Path path, final BlockingQueue fileListingBlockingQueue,final FileFilter filter, final boolean recurse) {
        Deque<Path> stack = new ArrayDeque<>();
        DirectoryStream<Path> stream = null;

        try {
            if (!Files.isWritable(path) || !Files.isReadable(path)) {
                eHandle.getAndSet("Directory '" + path + "' does not have sufficient permissions (i.e., not writable and readable)");
                return;
            }

            stack.push(path);
            notFinished.set(true);
            listSizeNotMet.set(true);
            while (!stack.isEmpty() && eHandle.get() == null && listSizeNotMet.get()) {
                stream = Files.newDirectoryStream(stack.pop());
                for (java.nio.file.Path entry : stream) {
                    if (!Files.isWritable(path) || !Files.isReadable(path)) {
                        eHandle.getAndSet("Directory '" + path + "' does not have sufficient permissions (i.e., not writable and readable)");
                        return;
                    }
                    if (Files.isDirectory(entry)) {
                        if(processHiddenDirectories.get() || !Files.isHidden(entry))
                        stack.push(entry);
                    } else {
                        if(listSizeNotMet.get()) {
                            if (filter.accept(entry.toFile())) {
                                while (fileListingBlockingQueue.remainingCapacity() <= 0) {
                                    getLogger().debug("Read queue has reached capacity");
                                    try {
                                        Thread.sleep(2000);
                                    } catch (Exception ignored) {
                                    }
                                }
                                fileListingBlockingQueue.add(entry.toFile());
                            }
                        } else
                            break;

                    }

                }
                stream.close();
            }
        } catch (Exception ignored) {
        } finally {
            try {
                if (null != stream) {
                    stream.close();
                }
            } catch (Exception ignore) {
            }
            notFinished.set(false);
        }
    }

    protected Map<String, String> getAttributesFromFile(final Path file) {
        Map<String, String> attributes = new HashMap<>();
        try {
            FileStore store = Files.getFileStore(file);
            if (store.supportsFileAttributeView("basic")) {
                try {
                    final DateFormat formatter = new SimpleDateFormat(FILE_MODIFY_DATE_ATTR_FORMAT, Locale.US);
                    BasicFileAttributeView view = Files.getFileAttributeView(file, BasicFileAttributeView.class);
                    BasicFileAttributes attrs = view.readAttributes();
                    attributes.put(FILE_LAST_MODIFY_TIME_ATTRIBUTE, formatter.format(new Date(attrs.lastModifiedTime().toMillis())));
                    attributes.put(FILE_CREATION_TIME_ATTRIBUTE, formatter.format(new Date(attrs.creationTime().toMillis())));
                    attributes.put(FILE_LAST_ACCESS_TIME_ATTRIBUTE, formatter.format(new Date(attrs.lastAccessTime().toMillis())));
                } catch (Exception ignore) {
                } // allow other attributes if these fail
            }
            if (store.supportsFileAttributeView("owner")) {
                try {
                    FileOwnerAttributeView view = Files.getFileAttributeView(file, FileOwnerAttributeView.class);
                    attributes.put(FILE_OWNER_ATTRIBUTE, view.getOwner().getName());
                } catch (Exception ignore) {
                } // allow other attributes if these fail
            }
            if (store.supportsFileAttributeView("posix")) {
                try {
                    PosixFileAttributeView view = Files.getFileAttributeView(file, PosixFileAttributeView.class);
                    attributes.put(FILE_PERMISSIONS_ATTRIBUTE, PosixFilePermissions.toString(view.readAttributes().permissions()));
                    attributes.put(FILE_GROUP_ATTRIBUTE, view.readAttributes().group().getName());
                } catch (Exception ignore) {
                } // allow other attributes if these fail
            }
        } catch (IOException ioe) {
            // well then this FlowFile gets none of these attributes
        }

        return attributes;
    }

   // @SuppressWarnings("checkstyle:LineLength")
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final File directory = new File(context.getProperty(DIRECTORY).evaluateAttributeExpressions().getValue());
        final boolean keepingSourceFile = context.getProperty(KEEP_SOURCE_FILE).asBoolean();
        final ComponentLog logger = getLogger();
        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        final int maxListSize = context.getProperty(LISTING_SIZE).asInteger();
        processHiddenDirectories.set(context.getProperty(HIDDEN_DIRECTORIES).asBoolean());
        boolean isProcessing = false;
        final int maxConcurrentTasks = context.getMaxConcurrentTasks();
        Thread listingThread;
        Thread processListThread;

        if (fileQueue.size() < 100 && !notFinished.get() && !processingList.get() ) {
            final long pollingMillis = context.getProperty(POLLING_INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS);
            if ((queueLastUpdated.get() < System.currentTimeMillis() - pollingMillis) && listingLock.tryLock()) {
                //Create and start thread to get the file listing
                processingList.set(true);
                isProcessing=true;
                try {
                    queueLock.lock();
                    notFinished.set(true);
                    processingList.set(true);
                    queueLock.unlock();

                    listingThread = startFileListing(context);
                    processListThread = startFileListProcessing(keepingSourceFile, batchSize, maxListSize);

                    while (eHandle.get() == null && batchSizeNotMet.get() && processingList.get()) {
                        try {
                            Thread.sleep(10); //Wait until listing complete or number of files available for processing is at least batch size
                        } catch (InterruptedException ignored) {

                        }
                    }
                    //If Exception occured unlock and throw
                    if (eHandle.get() != null) {
                        listingLock.unlock();
                        try{
                            listingThread.interrupt();
                            processListThread.interrupt();
                        } catch(Exception ignored) {
                        }
                        throw new IllegalArgumentException(eHandle.get(),new IllegalStateException(eHandle.get()));
                    }

                    queueLock.lock();

                    try {

                        recentlyProcessed.forEach(i -> {
                            fileQueue.remove(i);tmpFileList.add(i);
                        });
                        if(!keepingSourceFile) {
                            tmpFileList.forEach(recentlyProcessed::remove);
                        }
                        queueLastUpdated.set(System.currentTimeMillis());
                    } finally {
                        tmpFileList.clear();
                        //noinspection CatchMayIgnoreException
                        try {
                            queueLock.unlock();
                        }catch(Exception ignoreNotLocked) {
                        }
                        try {
                            processLock.unlock();
                        }catch(Exception ignoreNotLocked) {
                        }
                    }
                } finally {
                    queueLock.lock();
                    isProcessing=false;
                    if(!keepingSourceFile){
                        recentlyProcessed.clear();
                    }
                    try {
                        queueLock.unlock();
                    } catch (Exception ignoreNotLocked) {
                    }
                    try {
                        listingLock.unlock();
                    } catch(Exception ignored) { //may have been released due to error/failsafe
                    }
                }
            }

            while (isProcessing && eHandle.get() == null && batchSizeNotMet.get() && processingList.get()) {
                try {
                    Thread.sleep(20); //Wait until listing complete or number of files available for processing is at least batch size
                } catch (InterruptedException ignored) {
                    logger.error("Unable to get file list successfully, list processing threads interrupted");
                }

            }
        }
        if(isProcessing && !notFinished.get() && !processingList.get()) {
            isProcessing =  false;
            try {
                listingLock.unlock();
            }catch(Exception ignoreNotLocked){

            }
        }

        final List<File> files = new ArrayList<>(batchSize);
        queueLock.lock();
        try {
            if(fileQueue.size()>0) {

                fileQueue.drainTo(files, batchSize > 100 ? batchSize : 100);
                if (files.isEmpty()) {
                    return;
                } else {
                    inProcess.addAll(files);
                }
            } else {
                if (fileQueue.isEmpty() && !processingList.get() && isProcessing) {
                    try {
                        queueLock.unlock();
                    }catch(Exception ignoreNotLocked){
                    }finally{
                        try {
                            processLock.unlock();
                        }catch(Exception ignoreNotLocked) {

                        }
                    }
                    context.yield();
                }
            }
        } finally {
            try {
                queueLock.unlock();
            }catch(Exception ignoreNotLocked){
            }finally {
                try {
                    processLock.unlock();
                } catch (Exception ignoreNotLocked) {

                }
            }
        }

        final ListIterator<File> itr = files.listIterator();
        FlowFile flowFile = null;
        try {
            final Path directoryPath = directory.toPath();
            while (itr.hasNext()) {
                final File file = itr.next();
                if(maxConcurrentTasks>1 && !file.exists()) continue; //Check due to multiple instances since listing concurrent
                final Path filePath = file.toPath();
                final Path relativePath = directoryPath.relativize(filePath.getParent());
                String relativePathString = relativePath.toString() + "/";
                if (relativePathString.isEmpty()) {
                    relativePathString = "./";
                }
                final Path absPath = filePath.toAbsolutePath();
                final String absPathString = absPath.getParent().toString() + "/";

                flowFile = session.create();
                final long importStart = System.nanoTime();
                flowFile = session.importFrom(filePath, keepingSourceFile, flowFile);
                final long importNanos = System.nanoTime() - importStart;
                final long importMillis = TimeUnit.MILLISECONDS.convert(importNanos, TimeUnit.NANOSECONDS);

                flowFile = session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), file.getName());
                flowFile = session.putAttribute(flowFile, CoreAttributes.PATH.key(), relativePathString);
                flowFile = session.putAttribute(flowFile, CoreAttributes.ABSOLUTE_PATH.key(), absPathString);
                Map<String, String> attributes = getAttributesFromFile(filePath);
                if (attributes.size() > 0) {
                    flowFile = session.putAllAttributes(flowFile, attributes);
                }

                session.getProvenanceReporter().receive(flowFile, file.toURI().toString(), importMillis);
                session.transfer(flowFile, REL_SUCCESS);
                logger.info("added {} to flow", new Object[]{flowFile});

                if (!isScheduled()) {  // if processor stopped, put the rest of the files back on the queue.
                    queueLock.lock();
                    try {
                        while (itr.hasNext()) {
                            final File nextFile = itr.next();
                            if(nextFile.exists()) {
                                fileQueue.add(nextFile);
                            }
                            inProcess.remove(nextFile);
                        }
                    } finally {
                        queueLock.unlock();
                    }
                }
            }
            session.commit();
        } catch (final Exception e) {
            logger.error("Failed to retrieve files due to {}", e);

            // anything that we've not already processed needs to be put back on the queue
            if (flowFile != null) {
                session.remove(flowFile);
            }
        } finally {
            queueLock.lock();
            try {
                recentlyProcessed.addAll(files);
                inProcess.removeAll(files);
            } finally {
                queueLock.unlock();
                while (isProcessing && processingList.get()) { //Cannot exit until listing complete
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ie) {
                        processingList.set(false);
                    }
                }

                try {
                    listingLock.unlock();
                } catch(Exception ignoreNotLocked) { //already unlocked
                }
            }

        }

    }
    private Thread startFileListing(final ProcessContext context) {
        try {
            queueLock.lock();
            notFinished.set(true);
            processingList.set(true);
            fileQueue.clear();
            queueLock.unlock();
            final Thread getFilesThread = new Thread(() -> {
                try {
                    getFiles(Paths.get(context.getProperty(DIRECTORY).evaluateAttributeExpressions().getValue()),
                    fileListingBlockingQueue,
                    fileFilterRef.get(),
                    context.getProperty(RECURSE).asBoolean());

                } catch(Exception ioe) {
                    getLogger().warn("Error retrieving file listing:"+ioe.getCause());
                } finally {
                    notFinished.set(false);
                }

            });
            getFilesThread.start();
            return getFilesThread;
        } catch(Exception threadIssue) {
            return null;
        }
    }
    private Thread startFileListProcessing(final boolean keepingSourceFile, final int batchSize, final int maxListSize) {
        final ConcurrentLinkedQueue<File> managedFileQueue = new ConcurrentLinkedQueue<>();
        final ConcurrentLinkedQueue<File> tmpQueue = new ConcurrentLinkedQueue<>();

        batchSizeNotMet.set(true);
        //Set up thread to gather results into fileQueue
        final Thread addFilesThread = new Thread(() -> {
            processingList.set(true);
            try {
                //while there is data and no error
                StopWatch swd = new StopWatch(true);
                while (notFinished.get() && eHandle.get() == null) {
                    if (fileListingBlockingQueue.size() > 0) {
                        if(processLock.tryLock()) {
                            File polled;
                            long batchcnt=0;
                            while((polled = fileListingBlockingQueue.poll())!=null&&batchcnt< batchSize) {
                                managedFileQueue.add(polled);
                                batchcnt++;
                            }
                            queueLock.lock();

                            recentlyProcessed.forEach(i -> {
                                if(managedFileQueue.contains(i)) {
                                    managedFileQueue.remove(i);
                                    tmpQueue.add(i);
                                }
                            });

                            if(!keepingSourceFile) {
                                tmpQueue.forEach(recentlyProcessed::remove);
                            }

                            tmpQueue.clear();
                            managedFileQueue.removeAll(inProcess);
                            fileQueue.addAll(managedFileQueue);
                            fileQueue.forEach(managedFileQueue::remove);
                            if (fileQueue.size() >= batchSize) {
                                batchSizeNotMet.set(false);
                            }
                            if(fileQueue.size()>=maxListSize)
                                listSizeNotMet.set(false);
                            queueLock.unlock();
                            processLock.unlock();

                        }
                    } else {
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException ignored) {
                        }

                    }

                }

                if (fileListingBlockingQueue.size() > 0) {
                    while(fileListingBlockingQueue.size()>0) {
                        if(processLock.tryLock()) {
                            long batchcnt=0;
                            File polled;
                            while((polled = fileListingBlockingQueue.poll())!=null&&batchcnt< batchSize) {
                                managedFileQueue.add(polled);
                                batchcnt++;
                            }

                            queueLock.lock();

                            if (!keepingSourceFile) {
                                recentlyProcessed.forEach(i->{
                                    managedFileQueue.remove(i);
                                    tmpQueue.add(i);
                                });
                                tmpQueue.forEach(recentlyProcessed::remove);
                                tmpQueue.clear();
                            }
                            managedFileQueue.removeAll(inProcess);
                            fileQueue.addAll(managedFileQueue);
                            if(fileQueue.size()>=maxListSize)
                                listSizeNotMet.set(false);
                            queueLock.unlock();
                            managedFileQueue.clear();
                            batchSizeNotMet.set(false);
                            processLock.unlock();
                        }
                    }
                    swd.stop();
                    getLogger().debug("Completed file list processing in:"+swd.getDuration(TimeUnit.MILLISECONDS));

                }
                processingList.set(false);

            } finally {
                batchSizeNotMet.set(false);
                managedFileQueue.clear();

                try {
                    managedFileQueue.clear();
                    queueLock.unlock();
                }catch(Exception ignoreNotLocked){

                }
                try {
                    processLock.unlock();
                }catch(Exception ignoreNotLocked) {

                }
            }
        });
        addFilesThread.start();
        return addFilesThread;
    }

}
