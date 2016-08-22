package com.spaniard.jms.client.listener;

import com.spaniard.jms.client.exception.JmsException;

import javax.jms.*;

import java.util.Enumeration;

/**
 * @author Alexander Nesterov
 * @version 1.0
 */
public class JmsReplicatorListener extends JmsAbstractMessageListener implements MessageListener {

    private Queue destinationQueue;

    public void setDestinationQueue(Queue destinationQueue) {
        this.destinationQueue = destinationQueue;
    }

    @Override
    protected void handleTextMessage(final TextMessage textMessage) throws JmsException {
        try {
            Destination replyToDestination = textMessage.getJMSReplyTo();
            jmsMessageSender.execute(destinationQueue != null ? destinationQueue : replyToDestination,
                    (session, messageProducer) -> {
                        final TextMessage newTextMessage = session.createTextMessage(textMessage.getText());
                        // copy properties
                        final Enumeration en = textMessage.getPropertyNames();
                        while (en.hasMoreElements()) {
                            final String element = en.nextElement().toString();
                            newTextMessage.setStringProperty(element, textMessage.getStringProperty(element));
                        }
                        newTextMessage.setJMSCorrelationID(textMessage.getJMSCorrelationID());
                        newTextMessage.setJMSReplyTo(textMessage.getJMSReplyTo());
                        if (textMessage.getJMSExpiration() > 0) {
                            // ttl is set
                            messageProducer.setTimeToLive(textMessage.getJMSExpiration() - textMessage.getJMSTimestamp());
                        }
                        messageProducer.send(newTextMessage);
                        return null;
                    });
        } catch (JMSException e) {
            throw new JmsException(e.getMessage(), e);
        }
    }

}
