/**
 * 
 */
package org.elasticsearch.zeromq.impl;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArrayList;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.zeromq.ZMQRestImpl;
import org.elasticsearch.zeromq.ZMQServerTransport;
import org.elasticsearch.zeromq.ZMQSocket;
import org.zeromq.ZMQ;
import org.zeromq.ZMQQueue;

/**
 * Implementation of {@link ZMQServerTransport} based on a Router-Dealer-Queue
 * pattern.
 * 
 * @author tlrx
 * 
 */
public class ZMQQueueServerImpl extends
		AbstractLifecycleComponent<ZMQServerTransport> implements
		ZMQServerTransport {

	final String routerBinding;

	final int nbWorkers;

	final String workersBinding;

	private final ZMQ.Context context;
	
	private final ZMQRestImpl client;

	private CopyOnWriteArrayList<ZMQSocket> sockets = new CopyOnWriteArrayList<ZMQSocket>();

	@Inject
	protected ZMQQueueServerImpl(Settings settings, NodeService nodeService, ZMQRestImpl client) {
		super(settings);
		this.client = client;
		
		logger.debug("Reading ØMQ transport layer settings...");

		routerBinding = settings.get("zeromq.router.bind", "tcp://localhost:9700");
		nbWorkers = settings.getAsInt("zeromq.workers.threads", 3);
		workersBinding = settings.get("zeromq.workers.bind", "inproc://es_zeromq_workers");

		logger.debug(
				"ØMQ settings [zeromq.router.bind={}, zeromq.workers.threads={}, zeromq.workers.bind={}]",
				routerBinding, nbWorkers, workersBinding);

		logger.info("Creating ØMQ server context...");
		context = ZMQ.context(1);

		try {
			nodeService.putNodeAttribute("zeromq_address", routerBinding);
		} catch (Exception e) {
			// ignore
		}
	}

	@Override
	protected void doStart() throws ElasticSearchException {

		logger.info("Starting ØMQ router socket...");
		ZMQ.Socket router = context.socket(ZMQ.XREP);
		router.bind(routerBinding);

		logger.info("Starting ØMQ dealer socket...");
		ZMQ.Socket dealer = context.socket(ZMQ.XREQ);
		dealer.bind(workersBinding);

		for (int i = 0; i < nbWorkers; i++) {

			logger.debug("Creating worker #{}", i);
			ZMQSocket worker = new ZMQSocket(logger, context, workersBinding, i, client);

			daemonThreadFactory(settings, "zeromq_worker_" + i).newThread(worker).start();

			sockets.add(worker);
		}

		logger.info("Starting ØMQ queue...");
		ZMQQueue queue = new ZMQQueue(context, router, dealer);
		queue.run();
	}

	@Override
	protected void doClose() throws ElasticSearchException {
		logger.debug("Closing ØMQ server...");
		Iterator<ZMQSocket> it = sockets.iterator();
		
		while (it.hasNext()) {
			it.next().close();			
		}

		context.term();
		logger.debug("ØMQ server closed.");

	}

	@Override
	protected void doStop() throws ElasticSearchException {
		logger.debug("Stopping ØMQ server...");
	}
}
