package org.elasticsearch.zeromq;

import org.elasticsearch.common.Bytes;
import org.elasticsearch.common.Unicode;
import org.elasticsearch.rest.support.AbstractRestRequest;
import org.elasticsearch.rest.support.RestUtils;
import org.elasticsearch.zeromq.exception.NoURIFoundZMQException;
import org.elasticsearch.zeromq.exception.UnsupportedMethodZMQException;
import org.elasticsearch.zeromq.exception.ZMQTransportException;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author tlrx
 * 
 */
public class ZMQRestRequest extends AbstractRestRequest {

	private final List<byte[]> parts;

	private Method method;

	private String uri;

	private String rawPath;

	private final Map<String, String> params;

	public ByteBuffer body;

	public ZMQRestRequest(String payload, List<byte[]> parts) {
		super();
		this.parts = parts;
		this.params = new HashMap<String, String>();

		parse(payload);
	}

	private void parse(String payload) {

		if (payload != null) {

			String[] s = payload.split("\\|");

            if(s.length <2){
                throw new ZMQTransportException("Invalid message format");
            }
			
			// Method
			String m = s[0];

			if ("GET".equalsIgnoreCase(m)) {
				this.method = Method.GET;
			} else if ("POST".equalsIgnoreCase(m)) {
				this.method = Method.POST;
			} else if ("PUT".equalsIgnoreCase(m)) {
				this.method = Method.PUT;
			} else if ("DELETE".equalsIgnoreCase(m)) {
				this.method = Method.DELETE;
			} else if ("OPTIONS".equalsIgnoreCase(m)) {
				this.method = Method.OPTIONS;
			} else if ("HEAD".equalsIgnoreCase(m)) {
				this.method = Method.HEAD;
			} else {
                throw new UnsupportedMethodZMQException(m);
            }

			// URI
			this.uri = s[1];

            if((this.uri == null) || ("".equals(this.uri)) || "null".equalsIgnoreCase(this.uri)){
                throw new NoURIFoundZMQException();
            }

			int pathEndPos = uri.indexOf('?');
			if (pathEndPos < 0) {
				this.rawPath = uri;
			} else {
				this.rawPath = uri.substring(0, pathEndPos);
				RestUtils.decodeQueryString(uri, pathEndPos + 1, params);
			}

			// Content
			int indexContent = payload.indexOf(ZMQSocket.SEPARATOR, m.length() + uri.length());
			body = ByteBuffer.wrap(payload.substring(indexContent+1).getBytes());
		}
	}

	@Override
	public Method method() {
		return this.method;
	}

	@Override
	public String uri() {
		return this.uri;
	}

	@Override
	public String rawPath() {
		return this.rawPath;
	}

	@Override
	public boolean hasContent() {
		return ((body != null) && (body.remaining() > 0));
	}

	@Override
	public boolean contentUnsafe() {
		return false;
	}

	@Override
	public byte[] contentByteArray() {
		if (body == null) {
			return Bytes.EMPTY_ARRAY;
		}
		return body.array();
	}

	@Override
	public int contentByteArrayOffset() {
		if (body == null) {
			return 0;
		}
		return body.arrayOffset() + body.position();
	}

	@Override
	public int contentLength() {
		if (body == null) {
			return 0;
		}
		return body.remaining();
	}

	@Override
	public String contentAsString() {
		if (body == null) {
			return "";
		}
		return Unicode.fromBytes(contentByteArray(), contentByteArrayOffset(),
				contentLength());
	}

	@Override
	public String header(String name) {
		return null;
	}

	@Override
	public boolean hasParam(String key) {
		return params.containsKey(key);
	}

	@Override
	public String param(String key) {

		String p = params.get(key);
		return p;
	}

	@Override
	public Map<String, String> params() {
		return params;
	}

	@Override
	public String param(String key, String defaultValue) {
		String value = params.get(key);
		if (value == null) {
			return defaultValue;
		}
		return value;
	}

}
