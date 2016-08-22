package com.spaniard.jms.client.core;

import com.spaniard.jms.client.core.support.JmsRootHandler;

/**
 * @author Alexander Nesterov
 * @version 1.0
 */
public interface JmsMessageHandler<Request, Response> extends JmsRootHandler {

    Response handle(Request modelObject);
}
