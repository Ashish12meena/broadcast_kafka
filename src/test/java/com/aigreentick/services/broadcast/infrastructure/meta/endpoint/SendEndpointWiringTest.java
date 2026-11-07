package com.aigreentick.services.broadcast.infrastructure.meta.endpoint;

import com.aigreentick.services.broadcast.application.port.out.MetaSendPort;
import com.aigreentick.services.broadcast.infrastructure.config.ProviderSendProperties;
import com.aigreentick.services.broadcast.infrastructure.meta.MetaCloudApiClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.bind.PropertySourcesPlaceholdersResolver;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.IOException;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** The send endpoints wire as the application wires them, from the shipped yaml (doc 47 R3). */
class SendEndpointWiringTest {

    @Configuration
    @EnableConfigurationProperties(ProviderSendProperties.class)
    @Import({MetaSendEndpoint.class, PinnacleSendEndpoint.class, SendEndpointRegistry.class, MetaCloudApiClient.class})
    static class Wiring {

        @Bean
        WebClient metaWebClient() {
            return WebClient.create("http://127.0.0.1:1/v23.0");
        }

        @Bean
        ObjectMapper objectMapper() {
            return new ObjectMapper();
        }
    }

    @Test
    void bothEndpointsRegisterAndTheClientIsTheSendPort() {
        new ApplicationContextRunner()
                .withUserConfiguration(Wiring.class)
                .withPropertyValues("broadcast.providers.pinnacle.base-url=https://partnersv1.pinbot.ai/v3")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    SendEndpointRegistry registry = context.getBean(SendEndpointRegistry.class);
                    assertThat(registry.find("META_DIRECT")).isPresent();
                    assertThat(registry.find("PINNACLE")).isPresent();
                    assertThat(context.getBean(MetaSendPort.class)).isInstanceOf(MetaCloudApiClient.class);
                });
    }

    @Test
    void aMissingPinnacleBlockFailsAtStartup() {
        new ApplicationContextRunner().withUserConfiguration(Wiring.class).run(context ->
                assertThat(context).hasFailed().getFailure().rootCause()
                        .hasMessageContaining("broadcast.providers.pinnacle is missing"));
    }

    @Test
    void theShippedYamlPointsPinnacleAtItsPartnerHost() throws IOException {
        MutablePropertySources sources = new MutablePropertySources();
        for (PropertySource<?> source : new YamlPropertySourceLoader()
                .load("application", new ClassPathResource("application.yml"))) {
            sources.addLast(source);
        }
        ProviderSendProperties properties = new Binder(ConfigurationPropertySources.from(sources),
                new PropertySourcesPlaceholdersResolver(sources))
                .bind("broadcast", ProviderSendProperties.class).get();

        ProviderSendProperties.Provider pinnacle = properties.find("PINNACLE").orElseThrow();
        assertThat(pinnacle.baseUrl()).isEqualTo("https://partnersv1.pinbot.ai/v3");
        assertThat(pinnacle.readTimeout()).isEqualTo(Duration.ofSeconds(15));
        assertThat(pinnacle.maxConnections()).isEqualTo(32);
    }
}
