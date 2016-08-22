package com.spaniard.jms.client.listener.rmq;

import com.spaniard.jms.client.exception.JmsException;
import com.spaniard.jms.client.listener.JmsAbstractMessageListener;
import com.rabbitmq.client.*;
import com.spaniard.jms.client.core.support.ClassName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

/**
 * @author Alexander Nesterov
 * @version 1.0
 */
public class RmqReplicatorListener extends JmsAbstractMessageListener implements MessageListener {

    private static final Logger logger = LoggerFactory.getLogger(ClassName.getClassName());

    private static final int DELIVERY_MODE = 2;
    private static final int PRIORITY = 0;

    private ConnectionFactory connectionFactory;

    private String destinationQueue;

    private Integer closeTimeout;

    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public void setDestinationQueue(String destinationQueue) {
        this.destinationQueue = destinationQueue;
    }


    public void setCloseTimeout(Integer closeTimeout) {
        this.closeTimeout = closeTimeout;
    }

    @Override
    protected void handleTextMessage(final TextMessage textMessage) throws JmsException {
        Connection connection = null;
        Channel channel = null;
        try {
            connection = connectionFactory.newConnection();
            connection.addBlockedListener(new RmqBlockedListener());
            channel = connection.createChannel();
            channel.queueDeclare(destinationQueue, true, false, false, null);

            final AMQP.BasicProperties properties = getJmsProperties(textMessage);
            final String message = textMessage.getText();
            logger.debug(String.format("Trying to send message [%s] to queue [%s]", message, destinationQueue));
            channel.basicPublish("", destinationQueue, properties, message.getBytes());
        } catch (IOException | TimeoutException | JMSException e) {
            throw new JmsException(e);
        } finally {
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException | TimeoutException e) {
                    /* NOP */
                }
            }
            if (connection != null) {
                try {
                    connection.close(Objects.nonNull(closeTimeout) ? closeTimeout : -1);
                } catch (IOException e) {
                    /* NOP */
                }
            }
        }
    }

    private AMQP.BasicProperties getJmsProperties(final TextMessage textMessage) throws JMSException {
        // copy properties
        Map<String, Object> headers = new HashMap<>();
        final Enumeration en = textMessage.getPropertyNames();
        while (en.hasMoreElements()) {
            final String element = en.nextElement().toString();
            headers.put(element, textMessage.getStringProperty(element));
        }
        // the same as MessageProperties.PERSISTENT_TEXT_PLAIN + added jms properties
        return new AMQP.BasicProperties.Builder()
                .headers(headers)
                .contentType("text/plain")
                .deliveryMode(DELIVERY_MODE)
                .priority(PRIORITY)
                .build();
    }

    private static class RmqBlockedListener implements BlockedListener {
        @Override
        public void handleBlocked(String reason) throws IOException {
            logger.debug("Blocking connection: " + reason);
        }

        @Override
        public void handleUnblocked() throws IOException {
            logger.debug("Unblocking connection...");
        }
    }

}
