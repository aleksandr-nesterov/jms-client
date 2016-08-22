package com.spaniard.jms.client.exception;

/**
 * @author Alexander Nesterov
 * @version 1.0
 */
public class JmsException extends Exception {
    public JmsException() {
        super();
    }

    public JmsException(String message) {
        super(message);
    }

    public JmsException(String message, Throwable cause) {
        super(message, cause);
    }

    public JmsException(Throwable cause) {
        super(cause);
    }

    protected JmsException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
