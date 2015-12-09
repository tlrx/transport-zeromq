package org.elasticsearch.zeromq.test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Date;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.zeromq.ZMQSocket;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zeromq.ZMQ;

public class ZMQTransportPluginTest {

   private static Node node = null;
   protected ZMQ.Socket socket = null;

   private static ZMQ.Context context = null;

   /*
    * ØMQ Socket binding adress, must be coherent with elasticsearch.yml config file 
    */
   private static final String address = "tcp://localhost:9800";

   @BeforeClass
   public static void setUpBeforeClass() throws Exception {
      // Instantiate an ES server
      node = NodeBuilder.nodeBuilder()
              .settings(
                      ImmutableSettings.settingsBuilder()
                      .put("es.config", "elasticsearch.yml")
              ).node();

      // Instantiate a ZMQ context
      context = ZMQ.context(1);
   }

   @AfterClass
   public static void tearDownAfterClass() throws Exception {
      if (node != null) {
         node.close();
      }

      try {
         context.term();
      } catch (Exception e2) {
         // ignore
      }
   }

   @Before
   public void setUp() {
      socket = context.socket(ZMQ.DEALER);
      socket.setHWM(100);
      socket.setRcvHWM(100);
      socket.setSndHWM(100);
      socket.connect(address);
      // Handshake
      try {
         Thread.sleep(100);
      } catch (Exception e) {
         Assert.fail("Handshake failed");
      }
   }

   @After
   public void tearDown() {
      try {
         if (null != socket) {
            socket.close();
            Thread.sleep(500);
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

      StringBuilder sb = new StringBuilder(method);
      sb.append(ZMQSocket.SEPARATOR).append(uri).append(ZMQSocket.SEPARATOR);

      if (json != null) {
         sb.append(json);
      }

      String result = null;
      try {
         socket.send(sb.toString().getBytes("UTF-8"), 0);

         byte[] response = socket.recv(0);
         result = new String(response, Charset.forName("UTF-8"));

      } catch (UnsupportedEncodingException e) {
         Assert.fail("Exception when sending/receiving message");
      }
      return result;
   }

   @Test
   public void testDeleteMissingIndex() {
      String response = sendAndReceive("DELETE", "/test-index-missing/", null);

      Assert.assertTrue(response.startsWith("404|NOT_FOUND|{\"error\":\"IndexMissingException[[test-index-missing] missing]\",\"status\":404}"));

   }

   @Test
   public void testCreateIndex() {
      String response = sendAndReceive("DELETE", "/books/", null);
      Assert.assertNotNull(response);

      response = sendAndReceive("PUT", "/books/", null);
      Assert.assertTrue(response.startsWith("200|OK|{\"acknowledged\":true}"));
   }

   @Test
   public void testMapping() throws IOException {
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
      System.out.println(response);
      Assert.assertTrue(response.startsWith("200|OK|{\"acknowledged\":true}"));
   }

   @Test
   public void testIndex() throws IOException {
      XContentBuilder book1 = jsonBuilder()
              .startObject()
              .field("title", "Les Misérables")
              .field("author", "Victor Hugo")
              .field("year", "1862")
              .field("publishedDate", new Date())
              .endObject();

      String response = sendAndReceive("PUT", "/books/book/1", book1.string());
      Assert.assertTrue(response.startsWith("201|CREATED|{\"_index\":\"books\",\"_type\":\"book\",\"_id\":\"1\",\"_version\":1,\"created\":true}"));

      XContentBuilder book2 = jsonBuilder()
              .startObject()
              .field("title", "Notre-Dame de Paris")
              .field("author", "Victor Hugo")
              .field("year", "1831")
              .field("publishedDate", new Date())
              .endObject();

      response = sendAndReceive("PUT", "/books/book/2", book2.string());
      Assert.assertTrue(response.startsWith("201|CREATED|{\"_index\":\"books\",\"_type\":\"book\",\"_id\":\"2\",\"_version\":1,\"created\":true}"));

      XContentBuilder book3 = jsonBuilder()
              .startObject()
              .field("title", "Le Dernier Jour d'un condamné")
              .field("author", "Victor Hugo")
              .field("year", "1829")
              .field("publishedDate", new Date())
              .endObject();

      response = sendAndReceive("POST", "/books/book", book3.string());
      Assert.assertNotNull("Response should not be null", response);
      Assert.assertTrue(response.startsWith("201|CREATED|{\"_index\":\"books\",\"_type\":\"book\""));
   }

	@Test
	public void testRefresh() throws IOException{
		String response = sendAndReceive("GET", "/_all/_refresh", null);
		Assert.assertTrue(response.startsWith("200|OK"));
	}
	
	@Test
	public void testSearch() throws IOException{
		String response = sendAndReceive("GET", "/_all/_search", "{\"query\":{\"match_all\":{}}}");
		Assert.assertTrue(response.contains("\"hits\":{\"total\":3"));
			
		response = sendAndReceive("GET", "_search", "{\"query\":{\"bool\":{\"must\":[{\"range\":{\"year\":{\"gte\":1820,\"lte\":1832}}}],\"must_not\":[],\"should\":[]}},\"from\":0,\"size\":50,\"sort\":[],\"facets\":{},\"version\":true}:");
		Assert.assertTrue(response.contains("\"hits\":{\"total\":2"));
	}

	@Test
	public void testGet() throws IOException{
		String response = sendAndReceive("GET", "/books/book/2", null);
		Assert.assertTrue(response.contains("Notre-Dame de Paris"));
	}
}
