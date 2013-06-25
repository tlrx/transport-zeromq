package org.elasticsearch.zeromq.test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.zeromq.ZMQSocket;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zeromq.ZMQ;

public class ZMQTransportPluginTest {

   private static Node node = null;
   private static ZMQ.Context context = null;
   /*
    * ØMQ Socket binding adress, must be coherent with elasticsearch.yml config file 
    */
   private static final String address = "tcp://localhost:9800";
   private static Integer recordsInserted = 0;

   @BeforeClass
   public static void setUpBeforeClass() throws Exception {
      // Instantiate an ES server
      node = NodeBuilder.nodeBuilder()
              .settings(
              ImmutableSettings.settingsBuilder()
              .put("es.config", "elasticsearch.yml")).node();

      // Instantiate a ZMQ context
      context = ZMQ.context(1);
   }

   @AfterClass
   public static void tearDownAfterClass() throws Exception {
      if (node != null) {
         node.close();
      }
      try {
         if (context != null) {
            // This is done in finalize, and will hang failed tests (not terribly helpful)
            //context.term();
         }
      } catch (Exception e2) {
         // ignore
      }
   }

   /**
    * Simple method to send & receive zeromq message
    *
    * @param method
    * @param uri
    * @param json
    * @return
    */
   private String sendAndReceive(String method, String uri, String json) {

      ZMQ.Socket socket;
      socket = getSocket();

      sendOnly(method, uri, json, socket);

      String result;
      result = recvOnly(socket);

      closeSocket(socket);

      return result;
   }

   /**
    * Get a connected ZMQ socket
    *
    * @return
    */
   private ZMQ.Socket getSocket() {
      ZMQ.Socket socket = context.socket(ZMQ.DEALER);
      socket.connect(address);

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
         recordsInserted++;
      }

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

   @Test
   public void testAsyncronousSendAndReceiveOneThread() throws InterruptedException, ExecutionException {

      System.out.println("testAsyncronousSendAndReceiveOneThread");
      ExecutorService pool = Executors.newFixedThreadPool(1);
      Set<Future<Integer>> returnSet;
      returnSet = new HashSet<>();

      Callable<Integer> job = new AsyncTestThread(address, "async_tests", "test");
      Future<Integer> returnValue = pool.submit(job);
      returnSet.add(returnValue);
      for (Future<Integer> future : returnSet) {
         recordsInserted += future.get();
      }

   }

   @Test
   public void testAsyncronousSendAndReceiveMultipleThread() throws InterruptedException, ExecutionException {
      System.out.println("testAsyncronousSendAndReceiveMultipleThread");
      ExecutorService pool = Executors.newFixedThreadPool(100);
      Set<Future<Integer>> returnSet;
      returnSet = new HashSet<>();

      for (Integer i = 0; i < 100; i++) {
         Callable<Integer> job = new AsyncTestThread(address, "async_tests", "test" + i.toString());
         Future<Integer> returnValue = pool.submit(job);
         returnSet.add(returnValue);
      }

      for (Future<Integer> future : returnSet) {
         recordsInserted += future.get();
      }
   }

   @Test
   public void testDeleteMissingIndex() {
      System.out.println("testDeleteMissingIndex");
      String response = sendAndReceive("DELETE", "/test-index-missing/", null);
      Assert.assertEquals("404|NOT_FOUND|{\"error\":\"IndexMissingException[[test-index-missing] missing]\",\"status\":404}", response);
   }

   @Test
   public void testCreateIndex() {
      System.out.println("testCreateIndex");
      String response = sendAndReceive("DELETE", "/books/", null);
      Assert.assertNotNull(response);

      response = sendAndReceive("PUT", "/books/", null);
      Assert.assertEquals("200|OK|{\"ok\":true,\"acknowledged\":true}", response);
   }

   @Test
   public void testMapping() throws IOException {
      System.out.println("testMapping");
      XContentBuilder mapping = jsonBuilder()
              .startObject()
              .startObject("book")
              .startObject("properties")
              .startObject("title")
              .field("type", "string")
              .field("analyzer", "french")
              .endObject()
              .startObject("author")
              .field("type", "string")
              .endObject()
              .startObject("year")
              .field("type", "integer")
              .endObject()
              .startObject("publishedDate")
              .field("type", "date")
              .endObject()
              .endObject()
              .endObject()
              .endObject();

      String response = sendAndReceive("PUT", "/books/book/_mapping", mapping.string());
      Assert.assertEquals("200|OK|{\"ok\":true,\"acknowledged\":true}", response);
   }

   @Test
   public void testIndex() throws IOException {
      System.out.println("testIndex");
      XContentBuilder book1 = jsonBuilder()
              .startObject()
              .field("title", "Les Misérables")
              .field("author", "Victor Hugo")
              .field("year", "1862")
              .field("publishedDate", new Date())
              .endObject();

      String response = sendAndReceive("PUT", "/books/book/1", book1.string());
      Assert.assertEquals("201|CREATED|{\"ok\":true,\"_index\":\"books\",\"_type\":\"book\",\"_id\":\"1\",\"_version\":1}", response);

      XContentBuilder book2 = jsonBuilder()
              .startObject()
              .field("title", "Notre-Dame de Paris")
              .field("author", "Victor Hugo")
              .field("year", "1831")
              .field("publishedDate", new Date())
              .endObject();

      response = sendAndReceive("PUT", "/books/book/2", book2.string());
      Assert.assertEquals("201|CREATED|{\"ok\":true,\"_index\":\"books\",\"_type\":\"book\",\"_id\":\"2\",\"_version\":1}", response);

      XContentBuilder book3 = jsonBuilder()
              .startObject()
              .field("title", "Le Dernier Jour d'un condamné")
              .field("author", "Victor Hugo")
              .field("year", "1829")
              .field("publishedDate", new Date())
              .endObject();

      response = sendAndReceive("POST", "/books/book", book3.string());
      Assert.assertNotNull("Response should not be null", response);
      Assert.assertTrue(response.startsWith("201|CREATED|{\"ok\":true,\"_index\":\"books\",\"_type\":\"book\",\"_id\""));
   }

   @Test
   public void testRefresh() throws IOException {
      System.out.println("testRefresh");
      String response = sendAndReceive("GET", "/_all/_refresh", null);
      Assert.assertTrue(response.startsWith("200|OK"));
   }

   @Test
   public void testSearch() throws IOException {
      System.out.println("testSearch");
      String response = sendAndReceive("GET", "/_all/_search", "{\"query\":{\"match_all\":{}}}");
      System.out.println(response);
      Assert.assertTrue(response.contains("\"hits\":{\"total\":" + recordsInserted.toString()));

      response = sendAndReceive("GET", "_search", "{\"query\":{\"bool\":{\"must\":[{\"range\":{\"year\":{\"gte\":1820,\"lte\":1832}}}],\"must_not\":[],\"should\":[]}},\"from\":0,\"size\":50,\"sort\":[],\"facets\":{},\"version\":true}:");
      Assert.assertTrue(response.contains("\"hits\":{\"total\":2"));
   }

   @Test
   public void testGet() throws IOException {
      System.out.println("testGet");
      String response = sendAndReceive("GET", "/books/book/2", null);
      Assert.assertTrue(response.contains("Notre-Dame de Paris"));
   }
}
