package com.spaniard.jms.client.exception;

/**
 * @author Alexander Nesterov
 * @version 1.0
 */
public class UnmarshallerException extends RuntimeException {

    public UnmarshallerException() {
        super();
    }

    public UnmarshallerException(String message) {
        super(message);
    }

    public UnmarshallerException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnmarshallerException(Throwable cause) {
        super(cause);
    }

    protected UnmarshallerException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
