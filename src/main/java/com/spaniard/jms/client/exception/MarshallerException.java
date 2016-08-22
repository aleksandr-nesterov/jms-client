package com.spaniard.jms.client.exception;

/**
 * @author Alexander Nesterov
 * @version 1.0
 */
public class MarshallerException extends RuntimeException {

    public MarshallerException() {
        super();
    }

    public MarshallerException(String message) {
        super(message);
    }

    public MarshallerException(String message, Throwable cause) {
        super(message, cause);
    }

    public MarshallerException(Throwable cause) {
        super(cause);
    }

    protected MarshallerException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
