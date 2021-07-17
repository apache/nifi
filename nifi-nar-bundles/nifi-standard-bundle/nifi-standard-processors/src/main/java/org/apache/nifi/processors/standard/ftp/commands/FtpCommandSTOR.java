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
package org.apache.nifi.processors.standard.ftp.commands;

import org.apache.ftpserver.command.AbstractCommand;
import org.apache.ftpserver.ftplet.DataConnection;
import org.apache.ftpserver.ftplet.DataConnectionFactory;
import org.apache.ftpserver.ftplet.DefaultFtpReply;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.ftplet.FtpReply;
import org.apache.ftpserver.ftplet.FtpRequest;
import org.apache.ftpserver.impl.FtpIoSession;
import org.apache.ftpserver.impl.FtpServerContext;
import org.apache.ftpserver.impl.IODataConnectionFactory;
import org.apache.ftpserver.impl.LocalizedDataTransferFtpReply;
import org.apache.ftpserver.impl.LocalizedFtpReply;
import org.apache.ftpserver.impl.ServerFtpStatistics;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public class FtpCommandSTOR extends AbstractCommand {

    private static final Logger LOG = LoggerFactory.getLogger(FtpCommandSTOR.class);
    private final AtomicReference<ProcessSessionFactory> sessionFactory;
    private final CountDownLatch sessionFactorySetSignal;
    private final Relationship relationshipSuccess;

    public FtpCommandSTOR(AtomicReference<ProcessSessionFactory> sessionFactory, CountDownLatch sessionFactorySetSignal, Relationship relationshipSuccess) {
        this.sessionFactory = sessionFactory;
        this.sessionFactorySetSignal = sessionFactorySetSignal;
        this.relationshipSuccess = relationshipSuccess;
    }

    public void execute(final FtpIoSession ftpSession, final FtpServerContext context, final FtpRequest request) {
        try {
            executeCommand(ftpSession, context, request);
        } catch (DetailedFtpCommandException ftpCommandException) {
            ftpSession.write(LocalizedDataTransferFtpReply.translate(ftpSession, request, context,
                    ftpCommandException.getFtpReturnCode(),
                    ftpCommandException.getSubId(),
                    ftpCommandException.getMessage(),
                    ftpCommandException.getFtpFile()));
        } catch (FtpCommandException ftpCommandException) {
            ftpSession.write(new DefaultFtpReply(ftpCommandException.getFtpReturnCode(), ftpCommandException.getMessage()));
        } finally {
            ftpSession.resetState();
            ftpSession.getDataConnection().closeDataConnection();
        }
    }

    private void executeCommand(FtpIoSession ftpSession, FtpServerContext context, FtpRequest request)
            throws FtpCommandException {

        final String fileName = getArgument(request);

        checkDataConnection(ftpSession);

        final FtpFile ftpFile = getFtpFile(ftpSession, fileName);

        checkWritePermission(ftpFile);

        sendFileStatusOkay(ftpSession, context, request, ftpFile.getAbsolutePath());

        final DataConnection dataConnection = openDataConnection(ftpSession, ftpFile);

        transferData(dataConnection, ftpSession, context, request, ftpFile);
    }

    private String getArgument(final FtpRequest request) throws FtpCommandException {
        final String argument = request.getArgument();
        if (argument == null) {
            throw new DetailedFtpCommandException(FtpReply.REPLY_501_SYNTAX_ERROR_IN_PARAMETERS_OR_ARGUMENTS, "STOR", null, null);
        }
        return argument;
    }

    private void checkDataConnection(final FtpIoSession ftpSession) throws FtpCommandException {
        DataConnectionFactory dataConnectionFactory = ftpSession.getDataConnection();
        if (dataConnectionFactory instanceof IODataConnectionFactory) {
            InetAddress address = ((IODataConnectionFactory) dataConnectionFactory)
                    .getInetAddress();
            if (address == null) {
                throw new FtpCommandException(FtpReply.REPLY_503_BAD_SEQUENCE_OF_COMMANDS, "PORT or PASV must be issued first");
            }
        }
    }

    private FtpFile getFtpFile(final FtpIoSession ftpSession, final String fileName) throws FtpCommandException {
        FtpFile ftpFile = null;
        try {
            ftpFile = ftpSession.getFileSystemView().getFile(fileName);
        } catch (FtpException e) {
            LOG.error("Exception getting file object", e);
        }
        if (ftpFile == null) {
            throw new DetailedFtpCommandException(FtpReply.REPLY_550_REQUESTED_ACTION_NOT_TAKEN, "STOR.invalid", fileName, ftpFile);
        }
        return ftpFile;
    }

    private void checkWritePermission(final FtpFile ftpFile) throws FtpCommandException {
        if (!ftpFile.isWritable()) {
            throw new DetailedFtpCommandException(FtpReply.REPLY_550_REQUESTED_ACTION_NOT_TAKEN, "STOR.permission", ftpFile.getAbsolutePath(), ftpFile);
        }
    }

    private void sendFileStatusOkay(final FtpIoSession ftpSession, final FtpServerContext context, final FtpRequest request, final String fileAbsolutePath) {
        ftpSession.write(LocalizedFtpReply.translate(ftpSession, request, context,
                FtpReply.REPLY_150_FILE_STATUS_OKAY,
                "STOR",
                fileAbsolutePath)).awaitUninterruptibly(10000);
    }

    private DataConnection openDataConnection(final FtpIoSession ftpSession, final FtpFile ftpFile) throws FtpCommandException {
        final DataConnection dataConnection;
        try {
            dataConnection = ftpSession.getDataConnection().openConnection();
        } catch (Exception exception) {
            LOG.error("Exception getting the input data stream", exception);
            throw new DetailedFtpCommandException(FtpReply.REPLY_425_CANT_OPEN_DATA_CONNECTION,
                    "STOR",
                    ftpFile.getAbsolutePath(),
                    ftpFile);
        }
        return dataConnection;
    }

    private void transferData(final DataConnection dataConnection, final FtpIoSession ftpSession,
                              final FtpServerContext context, final FtpRequest request, final FtpFile ftpFile)
            throws FtpCommandException {

        final ProcessSession processSession;
        try {
            processSession = createProcessSession();
        } catch (InterruptedException|TimeoutException exception) {
            LOG.error("ProcessSession could not be acquired, command STOR aborted.", exception);
            throw new FtpCommandException(FtpReply.REPLY_425_CANT_OPEN_DATA_CONNECTION, "File transfer failed.");
        }
        FlowFile flowFile = processSession.create();
        long transferredBytes = 0L;
        try (OutputStream flowFileOutputStream = processSession.write(flowFile)) {
            transferredBytes = dataConnection.transferFromClient(ftpSession.getFtpletSession(), flowFileOutputStream);
            LOG.info("File received {}", ftpFile.getAbsolutePath());
        } catch (SocketException socketException) {
            LOG.error("Socket exception during data transfer", socketException);
            processSession.rollback();
            throw new DetailedFtpCommandException(FtpReply.REPLY_426_CONNECTION_CLOSED_TRANSFER_ABORTED,
                    "STOR",
                    ftpFile.getAbsolutePath(),
                    ftpFile);
        } catch (IOException ioException) {
            LOG.error("IOException during data transfer", ioException);
            processSession.rollback();
            throw new DetailedFtpCommandException(FtpReply.REPLY_551_REQUESTED_ACTION_ABORTED_PAGE_TYPE_UNKNOWN,
                    "STOR",
                    ftpFile.getAbsolutePath(),
                    ftpFile);
        }

        try {
            // notify the statistics component
            ServerFtpStatistics ftpStat = (ServerFtpStatistics) context.getFtpStatistics();
            ftpStat.setUpload(ftpSession, ftpFile, transferredBytes);

            processSession.putAttribute(flowFile, CoreAttributes.FILENAME.key(), ftpFile.getName());
            processSession.putAttribute(flowFile, CoreAttributes.PATH.key(), getPath(ftpFile));

            processSession.getProvenanceReporter().modifyContent(flowFile);

            processSession.transfer(flowFile, relationshipSuccess);
        } catch (Exception exception) {
            processSession.rollback();
            LOG.error("Process session error. ", exception);
        }

        final long byteCount = transferredBytes;
        processSession.commitAsync(() -> {
            // if data transfer ok - send transfer complete message
            ftpSession.write(LocalizedDataTransferFtpReply.translate(ftpSession, request, context,
                FtpReply.REPLY_226_CLOSING_DATA_CONNECTION, "STOR",
                ftpFile.getAbsolutePath(), ftpFile, byteCount));
        });
    }

    private String getPath(FtpFile ftpFile) {
        String absolutePath = ftpFile.getAbsolutePath();
        int endIndex = absolutePath.length() - ftpFile.getName().length();
        return ftpFile.getAbsolutePath().substring(0, endIndex);
    }

    private ProcessSession createProcessSession() throws InterruptedException, TimeoutException {
        ProcessSessionFactory processSessionFactory = getProcessSessionFactory();
        return processSessionFactory.createSession();
    }

    private ProcessSessionFactory getProcessSessionFactory() throws InterruptedException, TimeoutException {
        if (sessionFactorySetSignal.await(10000, TimeUnit.MILLISECONDS)) {
            return sessionFactory.get();
        } else {
            throw new TimeoutException("Waiting period for sessionFactory is over.");
        }
    }
}
