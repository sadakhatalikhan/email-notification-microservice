package com.navabitsolutions.email.notification.ms.handler;

import com.kafka.ws.core.ProductCreatedEvent;
import com.navabitsolutions.email.notification.ms.entity.ProcessedEventEntity;
import com.navabitsolutions.email.notification.ms.exceptions.NotRetryableException;
import com.navabitsolutions.email.notification.ms.exceptions.RetryableException;
import com.navabitsolutions.email.notification.ms.repository.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

@Component
@KafkaListener(topics="product-created-events-topic")
public class ProductCreatedEventHandler {

    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private RestTemplate restTemplate;
    @Autowired
    private ProcessedEventRepository repository;

    @Transactional
    @KafkaHandler
    public void handle(@Payload ProductCreatedEvent productCreatedEvent,
                       @Header("messageId") String messageId,
                       @Header(KafkaHeaders.RECEIVED_KEY) String messageKey) throws NotRetryableException {
      //  if(true) throw new NotRetryableException("An error took place. No need to consume the message again");
        LOGGER.info("Received a new event {}", productCreatedEvent.getTitle());

        ProcessedEventEntity existing = repository.findByMessageId(messageId);
        if (existing!=null) {
            LOGGER.warn("Found a duplicate messageId {}", existing.getMessageId());
            return;
        }

        String requestUrl = "http://localhost:8082/response/200";
        //String requestUrl = "http://localhost:8082/response/500";
        try {
            ResponseEntity<String> response = restTemplate.exchange(requestUrl, HttpMethod.GET, null, String.class);
            if (response.getStatusCode().value() == HttpStatus.OK.value()) {
                LOGGER.info("Received response from the remote service {}", response.getBody());
            }
        } catch(ResourceAccessException ex) {
            LOGGER.error(ex.getMessage());
            throw new RetryableException(ex);
        } catch (HttpServerErrorException ex) {
            LOGGER.error(ex.getMessage());
            throw new NotRetryableException(ex);
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage());
            throw new NotRetryableException(ex);
        }

        // Save unique message id into database
        try {
            repository.save(new ProcessedEventEntity(0, messageId, productCreatedEvent.getProductId()));
        } catch (DataIntegrityViolationException e) {
            throw new NotRetryableException(e);
        }
    }
}
