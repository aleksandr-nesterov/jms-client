package com.spaniard.jms.client.core;

import com.spaniard.jms.client.exception.JmsException;
import com.spaniard.jms.client.core.support.ClassName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * @author Alexander Nesterov
 * @version 1.0
 */

public class JmsFutureImpl<T> implements JmsFuture<T> {

    private static final Logger logger = LoggerFactory.getLogger(ClassName.getClassName());

    private enum State {
        WAITING, CANCELLED, DONE, ERROR
    }

    private final MessageConsumer messageConsumer;
    private final Connection connection;
    private final Session session;

    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();

    private volatile State state;
    private volatile T response;

    private final Class<T> responseClass;
    private final Function<String, Object> unmarshaller;

    public JmsFutureImpl(Connection connection, Session session, Destination replyTempQueue,
                         Class<T> responseClass, Function<String, Object> unmarshaller) throws JmsException {
        this.connection = connection;
        this.session = session;
        try {
            this.messageConsumer = session.createConsumer(replyTempQueue);
            this.messageConsumer.setMessageListener(this);
            state = State.WAITING;
        } catch (JMSException e) {
            throw new JmsException(e.getMessage(), e);
        }
        this.responseClass = responseClass;
        this.unmarshaller = unmarshaller;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (mayInterruptIfRunning && state == State.WAITING) {
            state = State.CANCELLED;
            // notify
            awaken();
            return true;
        }
        return false;
    }

    @Override
    public boolean isCancelled() {
        return state == State.CANCELLED;
    }

    @Override
    public boolean isDone() {
        return state == State.DONE;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        lock.lock();
        try {
            while (response == null && state != State.CANCELLED && state != State.ERROR) {
                condition.await();
            }
            state = State.DONE;
            return response;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        long nanos = unit.toNanos(timeout);
        lock.lock();
        try {
            while (response == null && state != State.CANCELLED && state != State.ERROR) {
                if (nanos <= 0L)
                    break;
                nanos = condition.awaitNanos(nanos);
            }
            state = State.DONE;
            return response;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void onMessage(Message message) {
        if (message instanceof TextMessage) {
            TextMessage textMessage = (TextMessage) message;
            try {
                final Object modelObject = unmarshaller.apply(textMessage.getText());
                response = castToClassResponse(modelObject, responseClass);
            } catch (Exception e) {
                state = State.ERROR;
                logger.error(e.getMessage(), e);
            } finally {
                // notify
                awaken();
            }
        }
    }

    private T castToClassResponse(final Object modelObject, Class<T> classResponse) {
        return classResponse.cast(modelObject);
    }

    private void awaken() {
        lock.lock();
        try {
            condition.signal();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        try {
            if (messageConsumer != null) {
                messageConsumer.close();
            }
        } catch (JMSException e) {
            logger.error(e.getMessage(), e);
        }
        try {
            if (session != null) {
                session.close();
            }
        } catch (JMSException e) {
            logger.error(e.getMessage(), e);
        }
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (JMSException e) {
            logger.error(e.getMessage(), e);
        }
    }
}
