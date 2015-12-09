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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.service.NodeService;

import static org.elasticsearch.common.util.concurrent.EsExecutors.*;

/**
 * @author tlrx
 */
public class ZMQServer extends AbstractLifecycleComponent<ZMQServer> {

	private final NodeService nodeService;

	private volatile ZMQServerTransport transport;

	@Inject
	public ZMQServer(Settings settings, NodeService nodeService,
			ZMQServerTransport transport, ZMQRestImpl client) {
		
		super(settings);
		this.transport = transport;
		this.nodeService = nodeService;
	}

	@Override
	protected void doStart() throws ElasticsearchException {

		logger.debug("Starting ØMQ server...");
		daemonThreadFactory(settings, "zeromq_server").newThread(
				new Runnable() {
					@Override
                    public void run() {
                        transport.start();
                    }
		}).start();
	}



	@Override
	protected void doStop() throws ElasticsearchException {
		nodeService.removeAttribute("zeromq_address");
        transport.stop();
	}

	@Override
	protected void doClose() throws ElasticsearchException {
		transport.close();
	}
}
