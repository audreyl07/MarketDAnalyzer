package dev.audreyl07.MDAnalyzer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Spring Boot entry point for MarketDAnalyzer.
 *
 * Responsibilities:
 * - Bootstraps the Spring application context
 * - Scans and wires controllers and services
 */
@SpringBootApplication
public class MdAnalyzerApplication {

	public static void main(String[] args) {
		SpringApplication.run(MdAnalyzerApplication.class, args);
	}

}
