/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.zeromq;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestResponse;
import org.zeromq.ZMQException;

/**
 * @author tlrx
 */
public class ZMQRestImpl extends AbstractComponent {

	private final RestController restController;

	@Inject
	public ZMQRestImpl(Settings settings, RestController restController) {
		super(settings);
		this.restController = restController;
	}

	public ZMQRestResponse process(ZMQRestRequest request){
		
		final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<ZMQRestResponse> ref = new AtomicReference<ZMQRestResponse>();
        
		this.restController.dispatchRequest(request, new RestChannel() {
			
			@Override
			public void sendResponse(RestResponse response) {
				try {
					if(logger.isTraceEnabled()){
						logger.info("Response to ØMQ client: {}", new String(response.content()));	
					}
					ref.set(convert(response));
				} catch (IOException e) {
					// ignore
				}
				latch.countDown();
			}
		});
		
		try {
            latch.await();
            return ref.get();
        } catch (Exception e) {
            throw new ZMQException("failed to generate response", 0);
        }
	}
	
	private ZMQRestResponse convert(RestResponse response) throws IOException {
		ZMQRestResponse zmqResponse = new ZMQRestResponse(response.status());

		if(response.contentType() != null){
			zmqResponse.setContentType(response.contentType());
		}
        if (response.contentLength() > 0) {
            if (response.contentThreadSafe()) {
            	zmqResponse.setBody(ByteBuffer.wrap(response.content(), 0, response.contentLength()));
            } else {
                // argh!, we need to copy it over since we are not on the same thread...
                byte[] body = new byte[response.contentLength()];
                System.arraycopy(response.content(), 0, body, 0, response.contentLength());
                zmqResponse.setBody(ByteBuffer.wrap(body));
            }
        }
        return zmqResponse;
    }
}
