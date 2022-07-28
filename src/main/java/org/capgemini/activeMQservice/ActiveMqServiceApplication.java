package org.capgemini.activeMQservice;

//import org.capgemini.catalogservice.model.Product;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.capgemini.catalogservice.model.Product;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
@EnableScheduling
public class ActiveMqServiceApplication {
    @Bean
    public Product product() {
        return new Product();
    }

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

    public static void main(String[] args) {
        SpringApplication.run(ActiveMqServiceApplication.class, args);
        System.out.print("GIT COMMIT MERGE THE PULL REQUEST  ");
    }

}

@Component
class ProducerController {

    final ObjectMapper objectMapper;
    final DiscoveryClient discoveryClient;
    final JmsTemplate jmsTemplate;
    final RestTemplate restTemplate;

    public ProducerController(ObjectMapper objectMapper, DiscoveryClient discoveryClient, JmsTemplate jmsTemplate, RestTemplate restTemplate) {
        this.objectMapper = objectMapper;
        this.discoveryClient = discoveryClient;
        this.jmsTemplate = jmsTemplate;
        this.restTemplate = restTemplate;
    }

    // @Scheduled(cron = "*/5 * * * * *")
    public void getMsg() {
        System.out.println("producer calling....");
        jmsTemplate.convertAndSend("consumer-dest", "hello world");
    }

    @Scheduled(cron = "*/5 * * * * *")
    public void getProduct() throws JsonProcessingException {
        List<ServiceInstance> catalogService = discoveryClient.getInstances("CATALOGSERVICE");
        ServiceInstance serviceInstance = catalogService.get(0);
        URI uri = serviceInstance.getUri();
        ResponseEntity<List<Product>> response = restTemplate
                .exchange(uri, HttpMethod.GET, HttpEntity.EMPTY, new ParameterizedTypeReference<List<Product>>() {
                });
        String s = objectMapper.writeValueAsString(response.getBody());
        jmsTemplate.convertAndSend("consumer-dest", s);
    }
}

@Component
class Consumer {
    final ObjectMapper objectMapper;

    Consumer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @JmsListener(destination = "consumer-dest")
    public void consume(String msg) {
        try {
            List<Product> products = objectMapper.readValue(msg, new TypeReference<>() {
            });
            Map<Long, List<String>> collect = products.stream().collect(Collectors.groupingBy(Product::getId,
                    Collectors.mapping(Product::getDescription, Collectors.toList())));
            System.out.println("consumer consumed:: " + collect);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }
}
