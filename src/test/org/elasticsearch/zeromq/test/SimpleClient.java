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
	 * @param args
	 */
	public static void main(String[] args) {

		if (args == null || args.length < 3) {
			System.err
					.println("Usage: SimpleClient <address> <method> <url> <json>");
			return;
		}

		// tcp://localhost:9700 by default
		String address = args[0];
		String method = args[1];
		String url = args[2];
		String json = null;
		if(args.length > 3){
			json = args[3];	
		}
		

		final ZMQ.Context context = ZMQ.context(1);
		ZMQ.Socket socket = context.socket(ZMQ.XREQ);
		socket.connect(address);

		// Handshake
		try {
			Thread.sleep(1000);
		} catch (Exception e) {
			e.printStackTrace();
		}

		StringBuilder sb = new StringBuilder(method);
		sb.append(ZMQSocket.SEPARATOR);
		sb.append(url).append(ZMQSocket.SEPARATOR);
		
		if(json != null){
			sb.append(json);	
		}

		try {
			socket.send(sb.toString().getBytes("UTF-8"), 0);

			byte[] response = socket.recv(0);
			System.out.println("Response: \r\n" + new String(response, "UTF-8"));

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

		}
	}
}
