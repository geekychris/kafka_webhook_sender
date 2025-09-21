package com.example.kafkawebhooksender;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = "com.example.kafkawebhooksender")
public class KafkaWebhookSenderApplication {

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(KafkaWebhookSenderApplication.class);
        var context = app.run(args);
        
        // Debug: List all controller beans
        System.out.println("=== Controller Beans ===");
        String[] controllerBeans = context.getBeanDefinitionNames();
        for (String beanName : controllerBeans) {
            if (beanName.toLowerCase().contains("controller")) {
                System.out.println("Found controller bean: " + beanName + " -> " + context.getBean(beanName).getClass().getName());
            }
        }
        System.out.println("=========================");
    }
}