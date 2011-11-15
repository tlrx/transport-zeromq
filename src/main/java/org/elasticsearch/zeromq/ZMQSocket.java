package org.elasticsearch.zeromq;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.zeromq.exception.ZMQTransportException;
import org.elasticsearch.zeromq.impl.ZMQQueueServerImpl;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

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

    private final AtomicBoolean isRunning;
	
	public ZMQSocket(ESLogger logger, Context context, String workersBinding, int id, ZMQRestImpl client, AtomicBoolean isRunning) {
		super();
		this.context = context;
		this.workersBinding = workersBinding;
		this.id = id;
		this.logger = logger;
		this.client = client;
        this.isRunning = isRunning;
	}

	@Override
	public void run() {

		socket = context.socket(ZMQ.XREP);
		socket.connect(workersBinding);

        if (logger.isInfoEnabled()) {
            logger.info("ØMQ socket {} is listening...", id);
        }

        while (isRunning.get()) {

			// Reads all parts of the message
			List<byte[]> parts = new ArrayList<byte[]>();

			try {
				
				do {
					byte[] request = socket.recv(0);
					parts.add(request);
				}while (socket.hasReceiveMore());	
				
			} catch (ZMQException zmqe) {
				// Close the socket
				if(logger.isWarnEnabled()){
                    logger.warn("Exception when receiving message", zmqe);
                }
			}

            if(parts.isEmpty()){
                continue;
            }

			// Payload
			String payload = new String(parts.get(parts.size() - 1));
			
			if (logger.isDebugEnabled()) {
	            logger.debug("ØMQ socket {} receives message: {}", id, payload);
	        }

            if(ZMQQueueServerImpl.ZMQ_STOP_SOCKET.equals(payload)){
                continue;
            }

            // Stores the latest exception
            Exception lastException = null;

            ZMQRestRequest request = null;
            ZMQRestResponse response = null;
            try{
                // Construct an ES request
                request = new ZMQRestRequest(payload, parts);

                // Process the request
                response = client.process(request);

            }catch (Exception e){
                if(logger.isErrorEnabled()){
                    logger.error("Exception when processing ØMQ message", e);
                }
                response = null;
                lastException = e;
            }

			// Sends all the message parts back
			for(int i=0; i<(parts.size() - 1); i++){
				socket.send(parts.get(i), ZMQ.SNDMORE);	
			}

            // Sends the reply
            if (response != null) {
                socket.send(response.payload(), 0);
            } else if(lastException != null) {
                // An error occured
                socket.send(("Unable to process ØMQ message [" + lastException.getMessage() + "]").getBytes(), 0);
            } else {
                // Should not happen
                socket.send(("Unable to process ØMQ message").getBytes(), 0);
            }
		}

        // Close the socket
        if (logger.isDebugEnabled()) {
            logger.debug("Closing ØMQ socket {}", id);
        }

		try {
			socket.close();

            logger.debug("ØMQ socket {} is closed", id);
		} catch (Exception e) {
			logger.error("Exception when closing ØMQ socket", e);
		}
	}
}
