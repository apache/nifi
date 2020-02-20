package org.apache.nifi.cdc.postgresql.pgEasyReplication;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

import org.json.simple.JSONObject;
import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;

public class Stream {

    private PGReplicationStream repStream;
    private Long lastReceiveLSN;
    private Decode decode;

    public Stream(String pub, String slt) throws SQLException {
        this(pub, slt, null);
    }

    public Stream(String pub, String slt, Long lsn) throws SQLException {
        PGConnection pgcon = ConnectionManager.getReplicationConnection().unwrap(PGConnection.class);

        if(lsn == null) {
            this.repStream = pgcon.getReplicationAPI()
                    .replicationStream()
                    .logical()
                    .withSlotName(slt)
                    .withSlotOption("proto_version", "1")		// More details about pgoutput options: https://github.com/postgres/postgres/blob/master/src/backend/replication/pgoutput/pgoutput.c
                    .withSlotOption("publication_names", pub)
                    .withStatusInterval(1, TimeUnit.SECONDS)
                    .start();

        } else {	// Reading from LSN start position
            LogSequenceNumber startLSN = LogSequenceNumber.valueOf(lsn);

            this.repStream = pgcon.getReplicationAPI()
                    .replicationStream()
                    .logical()
                    .withSlotName(slt)
                    .withSlotOption("proto_version", "1")
                    .withSlotOption("publication_names", pub)
                    .withStatusInterval(1, TimeUnit.SECONDS)
                    .withStartPosition(startLSN)
                    .start();
        }
    }

    public Event readStream(boolean isSimpleEvent, boolean withBeginCommit)
            throws SQLException, InterruptedException, ParseException, UnsupportedEncodingException {

        LinkedList<String> changes = new LinkedList<String>();

        if (this.decode == null) {	// First read
            this.decode = new Decode();
            decode.loadDataTypes();
        }

        while (true) {
            ByteBuffer buffer = this.repStream.readPending();

            if (buffer == null) {
                break;
            }

            JSONObject json = new JSONObject();
            String change = "";

            if (isSimpleEvent) {
                change = this.decode.decodeLogicalReplicationMessageSimple(buffer, json, withBeginCommit).toJSONString();
            } else {
                change = this.decode.decodeLogicalReplicationMessage(buffer, json, withBeginCommit).toJSONString().replace("\\\"", "\"");
            }

            if (!change.equals("{}")) // Skip empty transactions
                changes.addLast(change);

            /* Feedback */
            this.repStream.setAppliedLSN(this.repStream.getLastReceiveLSN());
            this.repStream.setFlushedLSN(this.repStream.getLastReceiveLSN());
        }

        this.lastReceiveLSN = this.repStream.getLastReceiveLSN().asLong();

        return new Event(changes, this.lastReceiveLSN, isSimpleEvent, withBeginCommit, false);
    }

    public Long getLastReceiveLSN() {
        return this.lastReceiveLSN;
    }
}
