package com.spaniard.jms.client.core;

import com.spaniard.jms.client.core.support.JmsRootHandler;

/**
 * @author Alexander Nesterov
 * @version 1.0
 */
public interface JmsHandler<T> extends JmsRootHandler {

    void handle(T modelObject);
}
