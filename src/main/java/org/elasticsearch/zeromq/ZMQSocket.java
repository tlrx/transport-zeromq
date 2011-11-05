package org.elasticsearch.zeromq;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.common.logging.ESLogger;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQException;

/**
 * @author tlrx
 *
 */
public class ZMQSocket implements Runnable {

	public final static String SEPARATOR = "|";
	
	private final ESLogger logger;
	
	private ZMQ.Socket socket;

	private volatile ZMQ.Context context;

	final String workersBinding;
	
	final int id;

	private final ZMQRestImpl client;
	
	public ZMQSocket(ESLogger logger, Context context, String workersBinding, int id, ZMQRestImpl client) {
		super();
		this.context = context;
		this.workersBinding = workersBinding;
		this.id = id;
		this.logger = logger;
		this.client = client;
	}

	@Override
	public void run() {

		socket = context.socket(ZMQ.XREP);
		socket.connect(workersBinding);
		
		while (true) {

			// Reads all parts of the message
			List<byte[]> parts = new ArrayList<byte[]>();
			
			try {
				
				do {
					byte[] request = socket.recv(0);
					parts.add(request);
				}while (socket.hasReceiveMore());	
				
			} catch (ZMQException zmqe) {
				// Close the socket
				close();
				return;
			}

			// Payload
			String payload = new String(parts.get(parts.size() - 1));
			
			if (logger.isTraceEnabled()) {
	            logger.trace("ØMQ message {}", payload);
	        }
			
			ZMQRestRequest request = new ZMQRestRequest(payload, parts);
			ZMQRestResponse response = client.process(request);			
			
			// Sends all the message parts back
			for(int i=0; i<(parts.size() - 1); i++){
				socket.send(parts.get(i), ZMQ.SNDMORE);	
			}
			
			// Sends the reply
			socket.send(response.payload(), 0);
		}
	}

	public void close() {
		try {
			socket.close();
		} catch (Exception e) {
			logger.error("Exception when closing ØMQ socket", e);
		}
	}
}
