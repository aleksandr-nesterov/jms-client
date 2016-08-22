package com.spaniard.jms.client.core;

import javax.jms.MessageListener;
import java.util.concurrent.Future;

/**
 * @author Alexander Nesterov
 * @version 1.0
 */
public interface JmsFuture<T> extends Future<T>, MessageListener, AutoCloseable {

    void close();
}
