package com.spaniard.jms.client.core;

import com.spaniard.jms.client.core.support.marshalling.Marshaller;
import com.spaniard.jms.client.core.support.unmarshalling.Unmarshaller;
import com.spaniard.jms.client.exception.IllegalPropertyException;
import com.spaniard.jms.client.exception.JmsException;
import com.spaniard.jms.client.core.support.ClassName;
import com.spaniard.jms.client.core.support.unmarshalling.AbstractUnmarshaller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.jms.*;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static com.spaniard.jms.client.core.JmsMessageSender.SenderStrategy.*;
import static com.spaniard.jms.client.core.support.marshalling.AbstractMarshaller.MarshallerFactory;

/**
 * @author Alexander Nesterov
 * @version 1.0
 */
@Component
public class JmsMessageSender {

    private static final Logger logger = LoggerFactory.getLogger(ClassName.getClassName());

    // Property fields
    private ConnectionFactory connectionFactory;

    private ConnectionFactory xaConnectionFactory;

    private String modelPackage;

    // default is XML
    private Strategy strategy = Strategy.XML;

    private Map<String, Destination> destinations;

    private Map<String, String> properties;
    // default is XML
    private Strategy responseStrategy = Strategy.XML;

    private long expiration;

    // it is used in BeanPostProcessor, in order to avoid using jaxb ObjectFactory
    private Set<Class> modelClasses;

    public void setModelPackage(String modelPackage) {
        this.modelPackage = modelPackage;
    }

    /**
     * @param strategy -- strategy for marshalling
     */
    public void setStrategy(Strategy strategy) {
        this.strategy = strategy;
    }

    /**
     * @param destinations Destination map (ModelClass, Queue | Topic)
     */
    public void setDestinations(Map<String, Destination> destinations) {
        this.destinations = destinations;
    }

    /**
     * @param properties -- jms header properties
     */
    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    /**
     * @param responseStrategy -- response strategy for marshalling
     */
    public void setResponseStrategy(Strategy responseStrategy) {
        this.responseStrategy = responseStrategy;
    }

    /**
     * @param expiration -- jms message expiration time (ttl)
     */
    public void setExpiration(long expiration) {
        this.expiration = expiration;
    }

    /**
     * This connection factory is used for `request` and `asyncRequest` methods
     *
     * @param connectionFactory -- implementation of connection factory such as (ActiveMQConnectionFactory)
     */
    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    /**
     * This connection factory is used for `send(...)` methods
     *
     * @param xaConnectionFactory -- implementation of xa connection factory such as ActiveMQXAConnectionFactory
     */
    public void setXaConnectionFactory(ConnectionFactory xaConnectionFactory) {
        this.xaConnectionFactory = xaConnectionFactory;
    }

    /**
     * Sends modelObject to the destination. Returns JMSClientFuture for asynchronous manipulation.
     * JMSClientFuture waits for the reply (responseClass).
     * <p>
     * For instance:
     * try (JMSClientFuture<Clazz.class> future = jmsClientMessageSender.asyncRequest(message, Clazz.class)) {
     * // do some work...
     * Clazz instance = future.get();
     * }
     *
     * @param modelObject   -- Model class that should be send
     * @param responseClass -- Response class that should be received
     * @return Future with responseClass
     * @throws JmsException
     */
    public <T> JmsFuture<T> asyncRequest(final Object modelObject, final Class<T> responseClass) throws JmsException {
        try {
            final Sender sender = new Sender(REQUEST);
            // first create future with message consumer
            final JmsFuture<T> responseFuture = sender.createAndGetFuture(responseClass);
            final Destination destination = obtainDestination(modelObject);
            log(modelObject, destination);
            // send message with reply to
            sender.send(destination, modelObject);
            return responseFuture;
        } catch (JMSException e) {
            throw new JmsException(e.getMessage(), e);
        }
    }

    /**
     * Sends model class to the destination. Waits for the response class to be received and returns it back.
     *
     * @param modelObject   -- Model class that should be send
     * @param responseClass -- Response class that should be received
     * @return Response class
     * @throws JmsException
     */
    @Deprecated
    public <T> T request(final Object modelObject, final Class<T> responseClass) throws JmsException {
        try {
            final Sender sender = new Sender(REQUEST);
            // first create future with message consumer
            try (final JmsFuture<T> responseFuture = sender.createAndGetFuture(responseClass)) {
                final Destination destination = obtainDestination(modelObject);
                log(modelObject, destination);
                // send message with reply to
                sender.send(destination, modelObject);
                return responseFuture.get();
            }
        } catch (InterruptedException | ExecutionException | JMSException e) {
            throw new JmsException(e.getMessage(), e);
        }
    }

    /**
     * Sends model class to the destination. Waits for the response class to be received and returns it back.
     *
     * @param modelObject   -- Model class that should be send
     * @param responseClass -- Response class that should be received
     * @param timeout       -- the maximum time to wait
     * @param unit          -- the time unit of the timeout argument
     * @return Response class
     * @throws JmsException
     */
    public <T> T request(final Object modelObject, final Class<T> responseClass, long timeout, TimeUnit unit) throws JmsException {
        try {
            final Sender sender = new Sender(REQUEST);
            // first create future with message consumer
            try (final JmsFuture<T> responseFuture = sender.createAndGetFuture(responseClass)) {
                final Destination destination = obtainDestination(modelObject);
                logger.debug("Sending object [{}] to the destination [{}]", modelObject, destination);
                // send message with reply to
                sender.send(destination, modelObject);
                return responseFuture.get(timeout, unit);
            }
        } catch (InterruptedException | ExecutionException | TimeoutException | JMSException e) {
            throw new JmsException(e.getMessage(), e);
        }
    }

    /**
     * Sends model class to the destination. Waits for the response class to be received and returns it back.
     *
     * @param replyToDestination -- The destination for response message
     * @param modelObject      -- Model class that should be send
     * @param responseClass    -- Response class that should be received
     * @param timeout          -- the maximum time to wait
     * @param unit             -- the time unit of the timeout argument
     * @return Response class
     * @throws JmsException
     */
    public <T> T request(final Destination replyToDestination, final Object modelObject, final Class<T> responseClass,
                         long timeout, TimeUnit unit) throws JmsException {
        try {
            final Sender sender = new Sender(REPLY_TO_REQUEST, replyToDestination);
            // first create future with message consumer
            try (final JmsFuture<T> responseFuture = sender.createAndGetFuture(responseClass)) {
                final Destination destination = obtainDestination(modelObject);
                logger.debug("Sending object [{}] to the destination [{}]", modelObject, destination);
                // send message with reply to
                sender.send(destination, modelObject);
                return responseFuture.get(timeout, unit);
            }
        } catch (InterruptedException | ExecutionException | TimeoutException | JMSException e) {
            throw new JmsException(e.getMessage(), e);
        }
    }

    /**
     * Just sends ModelClass to the destination. Destination should be set in the destination Map.
     * Can be used in either way (inside Xa transaction or without transaction).
     *
     * @param modelObject -- Model class that should be send
     * @throws JmsException
     */
    public void send(final Object modelObject) throws JmsException {
        final Destination destination = obtainDestination(modelObject);
        try (Sender sender = new Sender(defineSenderStrategy())) {
            log(modelObject, destination);
            // send message
            sender.send(destination, modelObject);
        } catch (JMSException e) {
            throw new JmsException(e.getMessage(), e);
        }
    }

    /**
     * Just sends ModelClass to the destination with the jms priority. Destination should be set in the destination Map.
     * Can be used in either way (inside Xa transaction or without transaction).
     *
     * @param modelObject -- Model class that should be send
     * @param priority -- Jms priority
     * @throws JmsException
     */
    public void send(final Object modelObject, int priority) throws JmsException {
        final Destination destination = obtainDestination(modelObject);
        try (Sender sender = new Sender(defineSenderStrategy())) {
            log(modelObject, destination);
            // send message
            sender.send(destination, modelObject, priority);
        } catch (JMSException e) {
            throw new JmsException(e.getMessage(), e);
        }
    }

    /**
     * Just sends ModelClass to the destination. Destination should be set in the destination Map.
     * Can be used in either way (inside Xa transaction or without transaction).
     *
     * @param modelObject -- Model class that should be send
     * @param properties  -- jms properties
     * @throws JmsException
     */
    public void send(final Object modelObject, final Map<String, String> properties) throws JmsException {
        final Destination destination = obtainDestination(modelObject);
        try (Sender sender = new Sender(defineSenderStrategy())) {
            log(modelObject, destination);
            // send message
            sender.send(destination, modelObject, properties);
        } catch (JMSException e) {
            throw new JmsException(e.getMessage(), e);
        }
    }

    /**
     * Sends modelObject to the given Destination
     * Can be used in either way (inside Xa transaction or without transaction).
     *
     * @param destination -- javax.jms.Destination endpoint
     * @param modelObject -- Model class that should be send
     * @throws JmsException
     */
    public void send(final Destination destination, final Object modelObject) throws JmsException {
        try (Sender sender = new Sender(defineSenderStrategy())) {
            log(modelObject, destination);
            // send message
            sender.send(destination, modelObject);
        } catch (JMSException e) {
            throw new JmsException(e.getMessage(), e);
        }
    }

    /**
     * Sends modelObject to the given Destination
     * Can be used in either way (inside Xa transaction or without transaction).
     *
     * @param destination -- javax.jms.Destination endpoint
     * @param modelObject -- Model class that should be send
     * @param properties  -- jms properties
     * @throws JmsException
     */
    public void send(final Destination destination, final Object modelObject, final Map<String, String> properties) throws JmsException {
        try (Sender sender = new Sender(defineSenderStrategy())) {
            log(modelObject, destination);
            // send message
            sender.send(destination, modelObject, properties);
        } catch (JMSException e) {
            throw new JmsException(e.getMessage(), e);
        }
    }

    /**
     * Sends modelObject to the given Destination
     * Can be used in either way (inside Xa transaction or without transaction).
     *
     * @param destination -- javax.jms.Destination endpoint
     * @param modelObject -- Model class that should be send
     * @param strategy    -- marshalling strategy
     * @throws JmsException
     */
    public void send(final Destination destination, final Object modelObject, final Strategy strategy) throws JmsException {
        try (Sender sender = new Sender(defineSenderStrategy())) {
            log(modelObject, destination);
            // send message
            sender.send(destination, modelObject, strategy);
        } catch (JMSException e) {
            throw new JmsException(e.getMessage(), e);
        }
    }

    /**
     * Sends modelObject to the given Destination
     * Can be used in either way (inside Xa transaction or without transaction).
     *
     * @param destination -- javax.jms.Destination endpoint
     * @param modelObject -- Model class that should be send
     * @param priority -- Jms priority
     * @param strategy    -- marshalling strategy
     * @throws JmsException
     */
    public void send(final Destination destination, final Object modelObject, final int priority, final Strategy strategy) throws JmsException {
        try (Sender sender = new Sender(defineSenderStrategy())) {
            log(modelObject, destination);
            // send message
            sender.send(destination, modelObject, priority, strategy);
        } catch (JMSException e) {
            throw new JmsException(e.getMessage(), e);
        }
    }

    /**
     * Sends modelObject to the given Destination
     * Can be used in either way (inside Xa transaction or without transaction).
     *
     * @param destination -- javax.jms.Destination endpoint
     * @param modelObject -- Model class that should be send
     * @param properties  -- jms properties
     * @param strategy    -- marshalling strategy
     * @throws JmsException
     */
    public void send(final Destination destination, final Object modelObject, final Map<String, String> properties, final Strategy strategy) throws JmsException {
        try (Sender sender = new Sender(defineSenderStrategy())) {
            log(modelObject, destination);
            // send message
            sender.send(destination, modelObject, properties, strategy);
        } catch (JMSException e) {
            throw new JmsException(e.getMessage(), e);
        }
    }

    /**
     * Non-transaction use
     *
     * @param destination -- jms destination
     * @param jmsMessageProducerCallback --
     * @param <R>
     * @return
     * @throws JmsException
     */
    public <R> R execute(final Destination destination, final JmsMessageProducerCallback<R> jmsMessageProducerCallback) throws JmsException {
        try (Sender sender = new Sender(NON_XA)) {
            logger.debug("Producing message to the destination [{}]", destination);
            // send message
            return sender.execute(destination, jmsMessageProducerCallback);
        } catch (JMSException e) {
            throw new JmsException(e.getMessage(), e);
        }
    }

    private Destination obtainDestination(final Object modelObject) throws JmsException {
        if (destinations == null) {
            throw new JmsException("No 'destinations' are set");
        }
        final Destination destination = destinations.get(modelObject.getClass().getCanonicalName());
        if (destination == null) {
            throw new JmsException(String.format("No destination found for key [%s]", modelObject.getClass().getCanonicalName()));
        }
        // ok
        return destination;
    }

    private void log(final Object modelObject, final Destination destination) {
        logger.debug("Sending object [{}] to the destination [{}]", modelObject, destination);
    }

    private SenderStrategy defineSenderStrategy() {
        if (Objects.nonNull(xaConnectionFactory)) {
            return SenderStrategy.XA;
        }
        if (Objects.nonNull(connectionFactory)) {
            return SenderStrategy.NON_XA;
        }
        throw new IllegalPropertyException("ConnectionFactory and XaConnectionFactory properties are null");
    }

    enum SenderStrategy {
        // is used for `request` methods, where we need to create temp-queue
        REQUEST,
        // is used inside xa-transaction
        XA,
        // is used outside transaction
        NON_XA,
        // is used for `request` method where we pass reply to destination as an argument
        REPLY_TO_REQUEST
    }

    /**
     * Sender class opens Connection, Session in the constructor. Temp-queue is created depending on the strategy.
     * Connection, Session is closed inside the `execute(...)` method.
     */
    private class Sender implements JmsMessageSenderAutoClosable {

        private final Connection connection;
        private final Session session;
        private Destination replyToDestination;

        private Sender(SenderStrategy senderStrategy) throws JMSException {
            // TODO: instead of switch use MAP (switch statement looks ugly)
            switch (senderStrategy) {
                case REQUEST:
                    // use replyTo mechanism
                    if (Objects.isNull(connectionFactory)) {
                        throw new IllegalPropertyException("ConnectionFactory property is null");
                    }
                    connection = connectionFactory.createConnection();
                    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    connection.start();
                    replyToDestination = session.createTemporaryQueue();
                    break;
                case XA:
                    // should be used in Xa transaction
                    connection = xaConnectionFactory.createConnection();
                    session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
                    break;
                case NON_XA:
                default:
                    if (Objects.isNull(connectionFactory)) {
                        throw new IllegalPropertyException("ConnectionFactory property is null");
                    }
                    connection = connectionFactory.createConnection();
                    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    break;
            }
        }

        private Sender(SenderStrategy senderStrategy, Destination replyToDestination) throws JMSException {
            switch (senderStrategy) {
                case REPLY_TO_REQUEST:
                    // use replyTo mechanism
                    if (Objects.isNull(connectionFactory)) {
                        throw new IllegalPropertyException("ConnectionFactory property is null");
                    }
                    connection = connectionFactory.createConnection();
                    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    connection.start();
                    this.replyToDestination = replyToDestination;
                    break;
                default:
                    if (Objects.isNull(connectionFactory)) {
                        throw new IllegalPropertyException("ConnectionFactory property is null");
                    }
                    connection = connectionFactory.createConnection();
                    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    break;
            }
        }

        private void send(final Destination destination, final Object modelObject) throws JMSException {
            send(destination, marshall(modelObject, strategy), properties, Message.DEFAULT_PRIORITY);
        }

        private void send(final Destination destination, final Object modelObject, int priority) throws JMSException {
            send(destination, marshall(modelObject, strategy), properties, priority);
        }

        private void send(final Destination destination, final Object modelObject, final Map<String, String> properties) throws JMSException {
            send(destination, marshall(modelObject, strategy), properties, Message.DEFAULT_PRIORITY);
        }

        private void send(final Destination destination, final Object modelObject, final Strategy strategy) throws JMSException {
            send(destination, marshall(modelObject, strategy), properties, Message.DEFAULT_PRIORITY);
        }

        private void send(final Destination destination, final Object modelObject, final int priority, final Strategy strategy) throws JMSException {
            send(destination, marshall(modelObject, strategy), properties, priority);
        }

        private void send(final Destination destination, final Object modelObject, final Map<String, String> properties, final Strategy strategy) throws JMSException {
            send(destination, marshall(modelObject, strategy), properties, Message.DEFAULT_PRIORITY);
        }

        private void send(final Destination destination, final String message, final Map<String, String> properties, int priority) throws JMSException {
            execute(destination, (session, messageProducer) -> {
                // create text message
                final TextMessage textMessage = session.createTextMessage(message);
                if (replyToDestination != null) {
                    // correlationID
                    final String correlationId = getCorrelationId();
                    textMessage.setJMSCorrelationID(correlationId);
                    textMessage.setJMSReplyTo(replyToDestination);
                }
                // check whether we need properties
                if (properties != null) {
                    for (Map.Entry<String, String> entry : properties.entrySet()) {
                        textMessage.setStringProperty(entry.getKey(), entry.getValue());
                    }
                }
                messageProducer.setPriority(priority);
                // send message
                messageProducer.send(textMessage);
                return null;
            });
        }

        private <R> R execute(final Destination destination, final JmsMessageProducerCallback<R> jmsMessageProducerCallback) throws JMSException {
            MessageProducer messageProducer = null;
            try {
                messageProducer = session.createProducer(destination);
                if (expiration > 0) {
                    messageProducer.setTimeToLive(expiration);
                }
                return jmsMessageProducerCallback.execute(session, messageProducer);
            } finally {
                if (messageProducer != null) {
                    try {
                        messageProducer.close();
                    } catch (JMSException e) {
                        logger.error("Could not close message producer", e);
                    }
                }
            }
        }

        @Override
        public void close() {
            if (session != null) {
                try {
                    session.close();
                } catch (JMSException e) {
                    logger.error("Could not close jms session", e);
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (JMSException e) {
                    logger.error("Could not close jms connection", e);
                }
            }
        }

        private String marshall(final Object modelObject, final Strategy strategy) {
            final Marshaller marshaller = Objects.isNull(modelPackage)
                    ? MarshallerFactory.fromModelClasses(modelClasses)
                    : MarshallerFactory.fromModelPackage(modelPackage);
            return marshaller.marshall(strategy, modelObject);
        }

        private <T> JmsFuture<T> createAndGetFuture(final Class<T> responseClass) throws JmsException {
            Function<String, Object> unmarshallerFunction = (message) -> {
                Unmarshaller unmarshaller = Objects.isNull(modelPackage)
                        ? AbstractUnmarshaller.UnmarshallerFactory.fromModelClasses(modelClasses)
                        : AbstractUnmarshaller.UnmarshallerFactory.fromModelPackage(modelPackage);
                return unmarshaller.unmarshall(strategy, message);
            };
            return new JmsFutureImpl(connection, session, replyToDestination, responseClass, unmarshallerFunction);
        }

        private String getCorrelationId() {
            return UUID.randomUUID().toString();
        }
    }

    private interface JmsMessageSenderAutoClosable extends AutoCloseable {

        void close();
    }
}
