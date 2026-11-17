/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.config;

import com.nexussaga.metrics.DbLoadMetricsInterceptor;
import org.hibernate.cfg.AvailableSettings;
import org.springframework.boot.autoconfigure.orm.jpa.HibernatePropertiesCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * JPA wiring: registers the SQL-counting inspector and exposes a {@link TransactionTemplate}
 * for the orchestrator's programmatic transactions.
 */
@Configuration(proxyBeanMethods = false)
public class PersistenceConfiguration {

    @Bean
    public HibernatePropertiesCustomizer nexusSagaStatementInspector(DbLoadMetricsInterceptor interceptor) {
        return properties -> properties.put(AvailableSettings.STATEMENT_INSPECTOR, interceptor);
    }

    @Bean
    public TransactionTemplate transactionTemplate(PlatformTransactionManager transactionManager) {
        return new TransactionTemplate(transactionManager);
    }
}
