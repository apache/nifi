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
package org.apache.nifi.processors.standard.relp;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.processors.standard.relp.frame.RELPEncoder;
import org.apache.nifi.processors.standard.relp.frame.RELPFrame;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class RELPFrameProducer {

    public static final String OPEN_FRAME_DATA = "relp_version=0\nrelp_software=librelp,1.2.7,http://librelp.adiscon.com\ncommands=syslog";

    static final RELPFrame OPEN_FRAME = new RELPFrame.Builder()
            .txnr(1)
            .command("open")
            .dataLength(OPEN_FRAME_DATA.length())
            .data(OPEN_FRAME_DATA.getBytes(StandardCharsets.UTF_8))
            .build();

    static final RELPFrame CLOSE_FRAME = new RELPFrame.Builder()
            .txnr(3)
            .command("close")
            .dataLength(0)
            .data(new byte[0])
            .build();

    public static void main(String[] args) {
        if (args == null || args.length != 5) {
            System.err.println("USAGE: RELPFrameProducer <HOST> <PORT> <NUM_MSGS> <DELAY_INTERVAL> <DELAY_MILLIS>");
            System.exit(1);
        }

        final String host = args[0];
        final int port = Integer.parseInt(args[1]);
        final int numMessages = Integer.parseInt(args[2]);
        final int delayInterval = Integer.parseInt(args[3]);
        final long delay = Long.parseLong(args[4]);

        final RELPEncoder encoder = new RELPEncoder(StandardCharsets.UTF_8);

        Socket socket = null;
        try {
            socket = new Socket(host, port);

            try (final OutputStream out = new BufferedOutputStream(socket.getOutputStream())) {
                // send the open frame
                out.write(encoder.encode(OPEN_FRAME));

                // send the specified number of syslog messages
                for (int i=2; i < (numMessages+2); i++) {
                    final byte[] data = ("this is message # " + i).getBytes(StandardCharsets.UTF_8);

                    final RELPFrame syslogFrame = new RELPFrame.Builder()
                            .txnr(i)
                            .command("syslog")
                            .dataLength(data.length)
                            .data(data)
                            .build();

                    out.write(encoder.encode(syslogFrame));

                    if (i % delayInterval == 0) {
                        System.out.println("Sent " + i + " messages");
                        out.flush();
                        Thread.sleep(delay);
                    }
                }

                // send the close frame
                out.write(encoder.encode(CLOSE_FRAME));

            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        } catch (final IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(socket);
        }
    }

}
