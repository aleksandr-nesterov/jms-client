package com.spaniard.jms.client.core.support.unmarshalling;

import com.spaniard.jms.client.core.Strategy;

/**
 * @author Alexander Nesterov
 * @version 1.0
 */
public interface Unmarshaller {

    Object unmarshall(Strategy strategy, String message);

}
