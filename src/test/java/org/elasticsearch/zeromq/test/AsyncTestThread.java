/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.elasticsearch.zeromq.test;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import org.elasticsearch.zeromq.ZMQSocket;
import org.junit.Assert;
import org.zeromq.ZMQ;

/**
 *
 * @author root
 */
public class AsyncTestThread implements Callable<Integer> {

   private String mAddress;
   private String mUri;
   private String mIndex;
   private String mType;
   private ZMQ.Socket mSocket;
   private ZMQ.Context mContext = null;
   public Integer mRecordsInserted = 0;

   /**
    * Get a connected ZMQ socket
    *
    * @return
    */
   private ZMQ.Socket getSocket() {
      ZMQ.Socket socket = mContext.socket(ZMQ.DEALER);
      socket.connect(mAddress);

      // Handshake
      try {
         Thread.sleep(100);
      } catch (Exception e) {
         Assert.fail("Handshake failed");
      }
      return socket;
   }

   /**
    * Close an open ZMQ socket
    *
    * @param socket
    */
   private void closeSocket(ZMQ.Socket socket) {
      try {
         socket.close();
      } catch (Exception e2) {
         // ignore
      }
   }

   /**
    * Send the REST query over the socket, don't pull the response
    *
    * @param method
    * @param uri
    * @param json
    * @return
    */
   private void sendOnly(String method, String uri, String json, ZMQ.Socket socket) {

      StringBuilder sb = new StringBuilder(method);
      sb.append(ZMQSocket.SEPARATOR).append(uri).append(ZMQSocket.SEPARATOR);

      if (json != null) {
         sb.append(json);
      }
      try {
         socket.send(sb.toString().getBytes("UTF-8"), 0);
      } catch (UnsupportedEncodingException e) {
         Assert.fail("Exception when sending/receiving message");
      }
      if (method.equals("PUT") && json != null && !json.isEmpty()) {
         mRecordsInserted++;
      }

   }

   /**
    * Send a message on the internal socket
    * 
    * @param method
    * @param uri
    * @param json 
    */
   public void send(String method, String uri, String json) {
      sendOnly(method,uri,json,mSocket);
   }
   /**
    * Receive a response to a REST query previously sent
    *
    * @param method
    * @param uri
    * @param json
    * @param socket
    * @return
    */
   private String recvOnly(ZMQ.Socket socket) {

      String result;

      byte[] response = socket.recv(0);
      result = new String(response, Charset.forName("UTF-8"));

      return result;
   }

   /**
    * Receive on the internal socket 
    * @return 
    */
   public String recv() {
      return recvOnly(mSocket);
   }
   public AsyncTestThread(String address, String index, String type) {
      mAddress = address;
      mUri = "/" + index + "/" + type + "/";
      mIndex = index;
      mType = type;
      mContext = ZMQ.context(1);
      mSocket = getSocket();
   }

   @Override
   protected void finalize() throws Throwable {

      closeSocket(mSocket);
      try {
         mContext.term();
      } catch (Exception e2) {
         // ignore
      } finally {
         super.finalize();
      }
   }

   @Override
   public Integer call() {
      String method;
      method = "PUT";

      String jsonBase;
      jsonBase = "{\"test\":";
      Set<String> expectedReplies = new HashSet<>();
      Integer i = 0;
      for (; i < 10; i++) {
         String json = jsonBase + i.toString() + "}";
         //System.out.println("Sending " + method + "|" + mUri + i.toString() + "|" + json);
         sendOnly(method, mUri + i.toString(), json, mSocket);
         String expected;
         expected = "201|CREATED|{\"ok\":true,\"_index\":\"";
         expected += mIndex;
         expected += "\",\"_type\":\"";
         expected += mType;
         expected += "\",\"_id\":\"";
         expected += i.toString();
         expected += "\",\"_version\":1}";
         //System.out.println("Expecting " + expected);
         expectedReplies.add(expected);
      }
      String reply;
      i = 0;
      //System.out.println("receiving replies:");
      while (!expectedReplies.isEmpty()) {
         //System.out.println("receiving reply:" + i.toString());
         reply = recvOnly(mSocket);
         //System.out.println("Got " + reply);
         Assert.assertTrue(expectedReplies.remove(reply));
      }
      //System.out.println("Got everything");
      return mRecordsInserted;
   }
}
