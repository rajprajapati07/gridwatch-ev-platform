package com.gridwatch.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

/**
 * WebSocket configuration for live dashboard pushes.
 *
 * Architecture: instead of clients polling every N seconds for status,
 * the server pushes diffs when connector status changes. At 12,000 sites
 * with 5K events/s, polling would generate enormous unnecessary load.
 *
 * Topics:
 * - /topic/status/{operatorId}  — connector status changes (filtered by operator)
 * - /topic/faults/{operatorId}  — fault alerts
 *
 * See StatusConsumer for the push side.
 */
@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    @Override
    public void configureMessageBroker(MessageBrokerRegistry config) {
        // In-memory broker for simplicity.
        // In production: replace with Redis pub-sub broker relay for horizontal scaling.
        config.enableSimpleBroker("/topic");
        config.setApplicationDestinationPrefixes("/app");
    }

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        registry.addEndpoint("/ws")
            .setAllowedOriginPatterns("*")
            .withSockJS(); // SockJS fallback for non-WebSocket clients
    }
}
