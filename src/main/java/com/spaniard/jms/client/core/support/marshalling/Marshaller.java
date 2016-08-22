package com.spaniard.jms.client.core.support.marshalling;

import com.spaniard.jms.client.core.Strategy;

/**
 * @author Alexander Nesterov
 * @version 1.0
 */
public interface Marshaller {

    String marshall(Strategy strategy, Object modelObject);
}
