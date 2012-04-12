/**
 * 
 */
package org.elasticsearch.zeromq.test;

import java.io.UnsupportedEncodingException;

import org.elasticsearch.zeromq.ZMQSocket;
import org.zeromq.ZMQ;

/**
 * A simple ØMQ client (XREQ)
 * 
 * @author tlrx
 * 
 */
public class SimpleClient {

	/**
	 * Format a message
	 * @param method
	 * @return
	 * @throws UnsupportedEncodingException 
	 */
	private static byte[] format(String method, String url, String json, Integer count) throws UnsupportedEncodingException {
		StringBuilder sb = new StringBuilder(method);
		sb.append(ZMQSocket.SEPARATOR);
		if (count != null) {
			sb.append(url).append(count);
		} else {
			sb.append(url);
		}
		sb.append(ZMQSocket.SEPARATOR);

		if (json != null) {
			sb.append(json);
		}
		return sb.toString().getBytes("UTF-8");
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {

		if (args == null || args.length < 3) {
			System.err
					.println("Usage: SimpleClient <address> <method> <url> <json> <repeat time>");
			return;
		}

		// tcp://localhost:9700 by default
		String address = args[0];
		String method = args[1];
		String url = args[2];
		String json = null;
		Integer repeat = 1;
		if(args.length > 3){
			json = args[3];	
		}
		if(args.length > 4){
			repeat = Integer.parseInt(args[4]);	
		}
		
		final ZMQ.Context context = ZMQ.context(1);
		ZMQ.Socket socket = context.socket(ZMQ.DEALER);
		socket.connect(address);

		// Handshake
		try {
			Thread.sleep(1000);
		} catch (Exception e) {
			e.printStackTrace();
		}

		long startTime =  System.currentTimeMillis();
		try {			
			
			// Send one message
			if(repeat == 1){
				socket.send(format(method, url, json, null), 0);

				byte[] response = socket.recv(0);
				System.out.println("Response: \r\n" + new String(response, "UTF-8"));
		
			// Send a lot of messages
			} else {
				for (int i = 0; i < repeat; i++) {					
					byte[] message = format(method, url, json, i);
					socket.send(message, ZMQ.NOBLOCK);

					byte[] response = socket.recv(ZMQ.NOBLOCK);
					if ((response != null) && (response.length > 0)) {
						System.out.println("Response: \r\n" + new String(response, "UTF-8"));
					}
				}
			}
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} finally {
			try {
				socket.close();
			} catch (Exception e2) {
				// ignore
			}
			try {
				context.term();
			} catch (Exception e2) {
				// ignore
			}
			
			System.out.printf("%d message(s) sent in %dms", repeat, System.currentTimeMillis()-startTime);
		}
	}
}
