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
package org.apache.nifi.remote;

import java.io.IOException;
import java.util.Map;

import org.apache.nifi.remote.protocol.DataPacket;


/**
 * <p>
 * Provides a transaction for performing site-to-site data transfers.
 * </p>
 * 
 * <p>
 * A Transaction is created by calling the 
 * {@link org.apache.nifi.remote.client.SiteToSiteClient#createTransaction(TransferDirection) createTransaction(TransferDirection)} 
 * method of a {@link org.apache.nifi.remote.client.SiteToSiteClient SiteToSiteClient}. The resulting Transaction
 * can be used to either send or receive data but not both. A new Transaction must be created in order perform the
 * other operation.
 * </p>
 * 
 * <p>
 * The general flow of execute of a Transaction is as follows:
 * <ol>
 *      <li>Create the transaction as described above.</li>
 *      <li>Send data via the {@link #send(DataPacket)} method or receive data via the {@link #receive()} method. This method
 *          will be called 1 or more times. In the case of receive, this method should be called until the method returns {@code null},
 *          signifying that the remote instance is finished sending data. <b>Note:</b> <code>receive()</code> should not be
 *          called a second time without first fully consuming the stream from the previous Packet that was received.</li>
 *      <li>Confirm the transaction via the {@link #confirm()} method.</li>
 *      <li>Either complete the transaction via the {@link #complete(boolean)} method or cancel the transaction
 *          via the {@link #cancel()} method.</li>
 * </ol>
 * </p>
 * 
 * <p>
 * It is important that the Transaction be terminated in order to free the resources held
 * by the Transaction. If a Transaction is not terminated, its resources will not be freed and
 * if the Transaction holds connections from a connection pool, the connections in that pool
 * will eventually become exhausted. A Transaction is terminated by calling one of the following
 * methods:
 *  <ul>
 *      <li>{@link #complete(boolean)}</li>
 *      <li>{@link #cancel()}</li>
 *      <li>{@link #error()}</li>
 *  </ul>
 * </p>
 * 
 * <p>
 * If at any point an IOException is thrown from one of the methods of the Transaction, that Transaction
 * is automatically closed via a call to {@link #error()}.
 * </p>
 * 
 * <p>
 * The Transaction class should not be assumed to be thread-safe.
 * </p>
 */
public interface Transaction {

    /**
     * Sends information to the remote NiFi instance.
     * 
     * @param dataPacket the data packet to send
     * @throws IOException
     */
    void send(DataPacket dataPacket) throws IOException;
    
    /**
     * Sends the given byte array as the content of a {@link DataPacket} along with the
     * provided attributes
     * 
     * @param content
     * @param attributes
     * @throws IOException
     */
    void send(byte[] content, Map<String, String> attributes) throws IOException;
    
    /**
     * Retrieves information from the remote NiFi instance, if any is available. If no data is available, will return
     * {@code null}. It is important to consume all data from the remote NiFi instance before attempting to 
     * call {@link #confirm()}. This is because the sender is always responsible for determining when the Transaction
     * has finished. This is done in order to prevent the need for a round-trip network request to receive data for
     * each data packet.
     * 
     * @return the DataPacket received, or {@code null} if there is no more data to receive. 
     * @throws IOException
     */
    DataPacket receive() throws IOException;

    /**
     * <p>
     * Confirms the data that was sent or received by comparing CRC32's of the data sent and the data received.
     * </p>
     * 
     * <p>
     * Even if the protocol being used to send the data is reliable and guarantees ordering of packets (such as TCP),
     * it is still required that we confirm the transaction before completing the transaction. This is done as
     * "safety net" or a defensive programming technique. Mistakes happen, and this mechanism helps to ensure that if
     * a bug exists somewhere along the line that we do not end up sending or receiving corrupt data. If the
     * CRC32 of the sender and the CRC32 of the receiver do not match, an IOException will be thrown and both the
     * sender and receiver will cancel the transaction automatically.
     * </p>
     * 
     * <p>
     * If the {@link TransferDirection} of this Transaction is RECEIVE, this method will throw an Exception unless
     * all data from the remote instance has been consumed (i.e., a call to {@link #receive()} returns {@code null}).
     * </p>
     * 
     * <p>
     * If the {@link TransferDirection} of this Transaction is SEND, calling this method dictates that no more data will be
     * sent in this transaction. I.e., there will be no more calls to {@link #send(DataPacket)}.
     * </p>
     * 
     * @throws IOException
     */
	void confirm() throws IOException;
	
	/**
     * <p>
     * Completes the transaction and indicates to both the sender and receiver that the data transfer was
     * successful.
     * </p>
     * 
     * @throws IOException
     * 
     * @return a TransactionCompletion that contains details about the Transaction
     */
	TransactionCompletion complete() throws IOException;
	
	/**
	 * <p>
	 * Cancels this transaction, indicating to the sender that the data has not been successfully received so that
	 * the sender can retry or handle however is appropriate.
	 * </p>
	 * 
	 * @param explanation an explanation to tell the other party why the transaction was canceled.
	 * @throws IOException
	 */
	void cancel(final String explanation) throws IOException;
	
	
	/**
	 * <p>
	 * Sets the TransactionState of the Transaction to {@link TransactionState#ERROR}, and closes
	 * the Transaction. The underlying connection should not be returned to a connection pool in this case.
	 * </p>
	 */
	void error();
	
	
	/**
	 * Returns the current state of the Transaction.
	 * @return
	 * @throws IOException
	 */
	TransactionState getState() throws IOException;
	
	/**
	 * Returns a Communicant that represents the other side of this Transaction (i.e.,
	 * the remote NiFi instance)
	 * @return
	 */
	Communicant getCommunicant();
	
	
	public enum TransactionState {
	    /**
	     * Transaction has been started but no data has been sent or received.
	     */
		TRANSACTION_STARTED,
		
		/**
		 * Transaction has been started and data has been sent or received.
		 */
		DATA_EXCHANGED,
		
		/**
		 * Data that has been transferred has been confirmed via its CRC. Transaction is
		 * ready to be completed.
		 */
		TRANSACTION_CONFIRMED,
		
		/**
		 * Transaction has been successfully completed.
		 */
		TRANSACTION_COMPLETED,
		
		/**
		 * The Transaction has been canceled.
		 */
		TRANSACTION_CANCELED,
		
		/**
		 * The Transaction ended in an error.
		 */
		ERROR;
	}
}
