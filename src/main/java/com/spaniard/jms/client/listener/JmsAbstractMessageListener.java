package com.spaniard.jms.client.listener;

import com.spaniard.jms.client.core.JmsMessageSender;
import com.spaniard.jms.client.exception.JmsException;
import com.spaniard.jms.client.core.support.ClassName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

/**
 * @author Alexander Nesterov
 * @version 1.0
 */
public abstract class JmsAbstractMessageListener implements MessageListener {

    private static final Logger logger = LoggerFactory.getLogger(ClassName.getClassName());

    protected JmsMessageSender jmsMessageSender;

    /**
     * Jms message sender is required to send messages, such as sending back to replyTo queue
     *
     * @param jmsMessageSender -- JmsMessageSender bean
     */
    public void setJmsMessageSender(JmsMessageSender jmsMessageSender) {
        this.jmsMessageSender = jmsMessageSender;
    }

    @Override
    public void onMessage(Message message) {
        if (!(message instanceof TextMessage)) {
            throw new JmsRuntimeException("Received message is not of type 'TextMessage'");
        }
        final TextMessage textMessage = (TextMessage) message;
        try {
            handleTextMessage(textMessage);
        } catch (JmsException e) {
            // initiate transaction rollback
            throw new JmsRuntimeException(e);
        }
    }

    protected abstract void handleTextMessage(TextMessage textMessage) throws JmsException;

    class JmsRuntimeException extends RuntimeException {
        public JmsRuntimeException(String message) {
            super(message);
        }

        public JmsRuntimeException(Throwable cause) {
            super(cause);
        }
    }

}
