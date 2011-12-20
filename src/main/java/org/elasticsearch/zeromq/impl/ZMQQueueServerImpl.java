/**
 * 
 */
package org.elasticsearch.zeromq.impl;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.http.BindHttpException;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.transport.BindTransportException;
import org.elasticsearch.zeromq.ZMQRestImpl;
import org.elasticsearch.zeromq.ZMQServerTransport;
import org.elasticsearch.zeromq.ZMQSocket;
import org.elasticsearch.zeromq.network.ZMQAddressHelper;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import org.zeromq.ZMQQueue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.util.concurrent.EsExecutors.*;

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

    private ZMQ.Socket dealer;

    private ZMQ.Socket router;

    private  Thread queueThread;

    private final ZMQRestImpl client;

	private CopyOnWriteArrayList<ZMQSocket> sockets = new CopyOnWriteArrayList<ZMQSocket>();

    private final NetworkService networkService;

    private final NodeService nodeService;

    private volatile BoundTransportAddress boundAddress;

    private final AtomicBoolean isRunning;

    public static final String ZMQ_STOP_SOCKET = "stop";

	@Inject
	protected ZMQQueueServerImpl(Settings settings, NodeService nodeService, ZMQRestImpl client, NetworkService networkService) {
		super(settings);
		this.client = client;
        this.networkService = networkService;
        this.nodeService = nodeService;
		
		logger.debug("Reading ØMQ transport layer settings...");

		routerBinding = settings.get("zeromq.router.bind", "tcp://localhost:9700");
		nbWorkers = settings.getAsInt("zeromq.workers.threads", 3);
		workersBinding = settings.get("zeromq.workers.bind", "inproc://es_zeromq_workers");

		logger.debug(
				"ØMQ settings [zeromq.router.bind={}, zeromq.workers.threads={}, zeromq.workers.bind={}]",
				routerBinding, nbWorkers, workersBinding);

		logger.info("Creating ØMQ server context...");
		context = ZMQ.context(1);

        isRunning = new AtomicBoolean(true);
	}

	@Override
	protected void doStart() throws ElasticSearchException {

		logger.debug("Starting ØMQ dealer socket...");
		dealer = context.socket(ZMQ.XREQ);
		dealer.bind(workersBinding);

        InetSocketAddress bindAddress;
        try {
            bindAddress = new InetSocketAddress(networkService.resolveBindHostAddress(ZMQAddressHelper.getHostName(workersBinding)), ZMQAddressHelper.getPort(workersBinding));
        } catch (IOException e) {
            throw new BindHttpException("Failed to resolve host [" + workersBinding + "]", e);
        }

		for (int i = 0; i < nbWorkers; i++) {

			logger.debug("Creating worker #{}", i);
			ZMQSocket worker = new ZMQSocket(logger, context, workersBinding, i, client, isRunning);

			daemonThreadFactory(settings, "zeromq_worker_" + i).newThread(worker).start();

			sockets.add(worker);
		}

		logger.debug("Starting ØMQ router socket...");
		router = context.socket(ZMQ.XREP);
		router.bind(routerBinding);

        InetSocketAddress publishAddress;
        try {
            publishAddress = new InetSocketAddress(networkService.resolvePublishHostAddress(ZMQAddressHelper.getHostName(routerBinding)), ZMQAddressHelper.getPort(routerBinding));
        } catch (Exception e) {
            throw new BindTransportException("Failed to resolve publish address", e);
        }
        this.boundAddress = new BoundTransportAddress(new InetSocketTransportAddress(bindAddress), new InetSocketTransportAddress(publishAddress));

        if (logger.isInfoEnabled()) {
            logger.info("{}", this.boundAddress);
        }
        nodeService.putNodeAttribute("zeromq_address", this.boundAddress.publishAddress().toString());

        logger.debug("Starting ØMQ queue...");

        queueThread = new Thread(new ZMQQueue(context, router, dealer));
        try {
            queueThread.start();
        } catch (ZMQException zmqe) {
            if(logger.isTraceEnabled()){
                logger.trace("Exception occurs while queue was running", zmqe);
            }
        }

	}

	@Override
	protected void doClose() throws ElasticSearchException {
		logger.debug("Closing ØMQ server...");

        // After next incoming message, sockets will close themselves
        isRunning.set(false);

        // Let's send a foobar message
        for (int i = 0; i < nbWorkers; i++) {
            dealer.send(ZMQ_STOP_SOCKET.getBytes(), 0);
        }

        // Stops the queue
        try {
            queueThread.interrupt();
        } catch (ZMQException zmqe) {
            if(logger.isTraceEnabled()){
                logger.trace("Exception occurs while closing queue", zmqe);
            }
        }

        // Close dealer socket
        dealer.close();
        logger.debug("ØMQ dealer socket closed");

        router.close();
        logger.debug("ØMQ router socket closed");

		context.term();
		logger.info("ØMQ server closed.");
	}

	@Override
	protected void doStop() throws ElasticSearchException {
		logger.debug("Stopping ØMQ server...");
	}

    @Override public BoundTransportAddress boundAddress() {
        return boundAddress;
    }
}
