package com.spaniard.jms.client.core;

import com.spaniard.jms.client.core.support.ClassName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ErrorHandler;

/**
 * @author Alexander Nesterov
 * @version 1.0
 */
public class JmsMessageErrorHandler implements ErrorHandler {

    private static final Logger logger = LoggerFactory.getLogger(ClassName.getClassName());

    @Override
    public void handleError(Throwable t) {
        logger.error("Could not handle jms message, it would be redelivered or placed to DLQ", t);
    }
}
