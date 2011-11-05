package org.elasticsearch.zeromq;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.elasticsearch.common.Bytes;
import org.elasticsearch.rest.AbstractRestResponse;
import org.elasticsearch.rest.RestStatus;

/**
 * @author tlrx
 * 
 */
public class ZMQRestResponse extends AbstractRestResponse {

	private final RestStatus status;

	public ByteBuffer body;

	private String contentType;

	public ZMQRestResponse(RestStatus status) {
		super();
		this.status = status;
	}

	@Override
	public String contentType() {
		return contentType;
	}

	public ZMQRestResponse setBody(ByteBuffer body) {
		this.body = body;
		return this;
	}

	@Override
	public byte[] content() throws IOException {
		if (body == null) {
			return Bytes.EMPTY_ARRAY;
		}
		return body.array();
	}

	@Override
	public int contentLength() throws IOException {
		if (body == null) {
			return 0;
		}
		return body.remaining();
	}

	@Override
	public RestStatus status() {
		return status;
	}

	@Override
	public boolean contentThreadSafe() {
		return false;
	}

	public void setContentType(String contentType) {
		this.contentType = contentType;
	}

	/**
	 * @return the payload to reply to the client
	 * @throws IOException
	 */
	public byte[] payload() {

		// TODO optimise & challenge thoses lines...
		ByteBuffer bStatus = ByteBuffer.wrap(this.status.toString().getBytes());
		ByteBuffer bSep = ByteBuffer.wrap(ZMQSocket.SEPARATOR.getBytes());
		ByteBuffer bContent = null;
		
		try {
			bContent = ByteBuffer.wrap(content());
		} catch (Exception e) {
			bContent = ByteBuffer.wrap(e.getMessage().getBytes());
		}

		ByteBuffer payload = ByteBuffer.allocate(bStatus.limit() + bSep.limit() + bContent.limit());
		payload.put(bStatus);
		payload.put(bSep);
		payload.put(bContent);
		
		return payload.array();
	}
}
