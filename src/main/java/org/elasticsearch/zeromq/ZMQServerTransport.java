/**
 * 
 */
package org.elasticsearch.zeromq;

import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.transport.BoundTransportAddress;

/**
 * @author tlrx
 *
 */
public interface ZMQServerTransport extends LifecycleComponent<ZMQServerTransport> {

    BoundTransportAddress boundAddress();
}
