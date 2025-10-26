# MarketDAnalyzer

A Spring Boot application that analyzes market data and integrates with QuestDB for time-series storage and querying. This project provides REST APIs for data ingestion, analysis, and maintenance of market datasets with SQL-based workflows.

## Installation 

**Requirements:**
- Java 11+ (or the version configured in `pom.xml`)
- Maven (the project includes the Maven wrapper: `mvnw` / `mvnw.cmd`)
- (Optional) A running QuestDB instance if you want to execute writes/queries against QuestDB

**Installation steps:**

1) Clone or download the repository to your local machine.

2) Build the project using the Maven wrapper (Windows PowerShell):

```powershell
.\mvnw.cmd clean package
```

This compiles the code, runs tests, and produces a JAR file in the `target/` directory.

## How to Use

Once installed, you can run and interact with the application in several ways:

**1. Run in development mode:**

```powershell
.\mvnw.cmd spring-boot:run
```

This starts the application using the classpath and configuration from `src/main/resources`.

**2. Run the packaged JAR:**

```powershell
java -jar .\target\marketdanalyzer-*.jar
```

**3. Make HTTP requests to the REST APIs:**

The application exposes REST endpoints for data ingestion, analysis, and maintenance. Use an HTTP client (curl, Postman, or PowerShell) to interact with the APIs.

Example GET request (replace with actual endpoint path):

```powershell
Invoke-RestMethod -Uri http://localhost:8080/api/data -Method Get
```

Example POST request to submit market data:

```powershell
Invoke-RestMethod -Uri http://localhost:8080/api/data 
-Method Post -Body (@{ /* json payload */ } | ConvertTo-Json) -ContentType 'application/json'
```

**4. Use the controllers:**

- **`DataController`** — Submit market data, trigger processing, and query results
- **`MaintenanceController`** — Trigger maintenance jobs like backfills or dataset refresh

(Open the controller source files for exact HTTP paths and request/response formats.)

**5. IDE / Debug:**

Import the Maven project into IntelliJ IDEA or Eclipse. Run `MdAnalyzerApplication` as a Spring Boot app for interactive debugging.

## How to Configure

Application configuration is in `src/main/resources/application.yaml`. This file controls:

- QuestDB connection properties (URL, credentials, port)
- Application server port
- Data source settings
- Other Spring Boot properties

**To customize:**

1) Edit `src/main/resources/application.yaml` before building/running.

2) Or provide environment-specific overrides using Spring Boot's standard configuration mechanisms (environment variables, command-line arguments, or external config files).

**QuestDB setup (optional):**

For development, run QuestDB locally using Docker. Example `docker-compose.yml` (not included in this repo):

```yaml
version: '3.7'
services:
  questdb:
    image: questdb/questdb:latest
    ports:
      - "9000:9000"   # web console
      - "8812:8812"   # ILP
```

Point `application.yaml` to the QuestDB instance (e.g., `localhost:8812` or `localhost:9000`).

**Troubleshooting:**

- If the app cannot connect to QuestDB, verify the host/port in `application.yaml` and ensure QuestDB is reachable.
- Create an `application.yaml.example` with minimal required properties for easy setup.

## Project Structure

- `src/main/java/dev/audreyl07/MDAnalyzer`
  - `MdAnalyzerApplication.java` 
  - `controller/`
    - `DataController.java` 
    - `MaintenanceController.java`
  - `service/`
    - `DataService.java`
    - `MaintenanceService.java` 
    - `QuestDBService.java` 
- `src/main/resources/application.yaml` 
- `src/main/resources/script/` 
  - `analysis_market.sql`
  - `historical_d.sql`
  - `historical_raw_d.sql`
  - `indicator_d_52w.sql`
  - `indicator_d_MA.sql`
  - `indices_d.sql`
  - `indices_raw_d.sql`
- `src/test/java/dev/audreyl07/MDAnalyzer/` 

## How to Test

Run unit/integration tests using the Maven wrapper:

```powershell
.\mvnw.cmd test
```

Add new tests under `src/test/java` following the existing structure. The repository includes a test skeleton at `src/test/java/dev/audreyl07/MDAnalyzer/MdAnalyzerApplicationTests.java`.

## Contact

For questions about the code, open an issue or contact the repository owner.
