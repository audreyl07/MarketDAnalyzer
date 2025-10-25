# MarketDAnalyzer

A small Spring Boot application that analyzes market data and integrates with QuestDB for storage/querying.

## Summary

This project contains a Java Spring Boot application (entry point: `MdAnalyzerApplication`) that provides data ingestion, analysis, and maintenance capabilities for market datasets. It includes SQL scripts used for preparing and analyzing data, and a service to interact with QuestDB.

## Project layout

- `src/main/java/dev/audreyl07/MDAnalyzer`
	- `MdAnalyzerApplication.java` — Spring Boot entry point
	- `controller/`
		- `DataController.java` — endpoints for data ingestion and retrieval
		- `MaintenanceController.java` — endpoints for maintenance tasks (backfills, reindexes, etc.)
	- `service/`
		- `DataService.java` — business logic for processing market data
		- `MaintenanceService.java` — maintenance workflows
		- `QuestDBService.java` — QuestDB integration (writes/queries)
- `src/main/resources/application.yaml` — configuration
- `src/main/resources/script/` — SQL scripts used by the app
	- `analysis_market.sql`
	- `historical_d.sql`
	- `historical_raw_d.sql`
	- `indicator_d_52w.sql`
	- `indicator_d_MA.sql`
	- `indices_d.sql`
	- `indices_raw_d.sql`

## Requirements

- Java 11+ (or the version configured in `pom.xml`)
- Maven (the project includes the Maven wrapper: `mvnw` / `mvnw.cmd`)
- (Optional) A running QuestDB instance if you want to execute writes/queries against QuestDB

## Configuration

Application configuration is in `src/main/resources/application.yaml` (also copied to `target/classes/application.yaml` during build). Update connection properties (QuestDB URL, authentication, etc.) there before running if necessary.

## Build and run (Windows PowerShell)

Build the project and run tests:

```powershell
.\mvnw.cmd clean package
.\mvnw.cmd test
```

Run the Spring Boot application using the Maven wrapper (development):

```powershell
.\mvnw.cmd spring-boot:run
```

Or run the packaged JAR (after `package`):

```powershell
# Replace the jar name as produced by the build
java -jar .\target\*.jar
```

## Useful endpoints (high level)

The project exposes controllers under `controller/`. Typical responsibilities:

- `DataController` — submit market data, trigger processing, query results
- `MaintenanceController` — trigger maintenance jobs like backfills or dataset refresh

(Open the controller source files for exact HTTP paths and request/response formats.)

## SQL scripts

The `src/main/resources/script` folder contains SQL used by the analysis workflows. These are typically executed as part of data preparation and analysis jobs.

## Tests

Run unit/integration tests with:

```powershell
.\mvnw.cmd test
```

Add tests under `src/test/java` following the existing structure. The repository already contains a test skeleton at `src/test/java/dev/audreyl07/MDAnalyzer/MdAnalyzerApplicationTests.java`.

## How to run

Below are concrete steps to build, run, and test the application locally (Windows PowerShell). Adjust paths or flags as needed for your environment.

1) Build the project and run unit tests

```powershell
.\mvnw.cmd clean package
# or to just run tests
.\mvnw.cmd test
```

2) Run the application (development)

```powershell
.\mvnw.cmd spring-boot:run
```

This runs the app with the classpath and configuration from `src/main/resources`. The app uses `application.yaml` for its configuration (QuestDB connection, ports, etc.).

3) Run the packaged JAR

After `clean package`, run the produced JAR from `target`:

```powershell
# Replace the jar name if necessary
java -jar .\target\marketdanalyzer-*.jar
```

4) Environment and QuestDB

- Edit `src/main/resources/application.yaml` (or supply environment-specific overrides) to configure the QuestDB URL/credentials, application port, and any other properties.
- For development, you can run QuestDB locally (official Docker image) and point `application.yaml` to it. Example docker-compose (not included):

```yaml
version: '3.7'
services:
	questdb:
		image: questdb/questdb:latest
		ports:
			- "9000:9000"   # web console
			- "8812:8812"   # ILP
```

5) Example HTTP request (once the app is running)

Use an HTTP client (curl, Postman) to exercise endpoints. Inspect controller sources for exact paths. Example (replace host/port/path):

```powershell
# GET example (replace path)
Invoke-RestMethod -Uri http://localhost:8080/api/data -Method Get
# POST example (replace with actual payload and path)
Invoke-RestMethod -Uri http://localhost:8080/api/data -Method Post -Body (@{ /* json */ } | ConvertTo-Json) -ContentType 'application/json'
```

6) IDE / Debug

- Import the Maven project into IntelliJ IDEA or Eclipse. Run `MdAnalyzerApplication` as a Spring Boot app for a debugging session.

7) Troubleshooting

- If the app cannot connect to QuestDB, verify the host/port in `application.yaml` and that QuestDB is reachable from your host.
- If you need example config, create `application.yaml.example` showing the minimal properties (QuestDB URL, port, datasource) and copy it to `application.yaml` when testing.

## Contact

For questions about the code, open an issue or contact the repository owner.
