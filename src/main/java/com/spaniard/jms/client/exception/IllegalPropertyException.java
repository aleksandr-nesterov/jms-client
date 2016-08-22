package com.spaniard.jms.client.exception;

/**
 * @author Alexander Nesterov
 * @version 1.0
 */
public class IllegalPropertyException extends RuntimeException {

    public IllegalPropertyException() {
    }

    public IllegalPropertyException(String message) {
        super(message);
    }

    public IllegalPropertyException(String message, Throwable cause) {
        super(message, cause);
    }

    public IllegalPropertyException(Throwable cause) {
        super(cause);
    }

    public IllegalPropertyException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
