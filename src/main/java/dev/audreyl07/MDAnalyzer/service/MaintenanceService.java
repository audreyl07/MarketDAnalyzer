package dev.audreyl07.MDAnalyzer.service;

import io.micrometer.common.util.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * Service orchestrating maintenance and batch operations against QuestDB.
 *
 * Responsibilities:
 * - Import raw CSV files into staging tables
 * - Populate historical, indicator, and analysis tables via SQL window functions
 * - Update or recompute aggregates (52w breadth, moving average breadth)
 * - Provide utilities to query latest processed dates per table/type
 */
@Service
public class MaintenanceService {
    @Autowired
    QuestDBService questDBService;

    private Map<String, Object> getFalseMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("success", Boolean.FALSE);
        return map;
    }

    public String getLatestDate(String table, String type) {
        String condition;
        if ("analysis_market".equals(table) || "indicator_d_52w".equals(table)) {
            condition = StringUtils.isNotEmpty(type) ? "type = '" + type + "'" : "";
        } else {
            condition = null;
        }
        return questDBService.getLatestDate(table, condition);
    }

    public Map<String, Object> importRawFiles(String type) {
        String table;
        if ("d".equals(type)) {
            table = "historical_raw_d";
        } else if ("etf_d".equals(type)) {
            table = "historical_raw_etf_d";
        } else if ("indices_d".equals(type)) {
            table = "indices_raw_d";
        } else {
            return getFalseMap();
        }
        boolean truncated = questDBService.truncateTable(table);
        if (!truncated) {
            return getFalseMap();
        }
        return questDBService.importFiles(table);
    }


    public Map<String, Object> insertIntoHistorical(String type) {
        String sourceTable;
        String targetTable;
        String query;
        String historicalQuery = """
                INSERT INTO %s
                SELECT\s
                    replace(ticker, '%s', ''),
                    CASE WHEN per = 'D' THEN
                    to_timestamp(date, 'yyyyMMdd')
                    ELSE dateadd('h', -6, to_timestamp(concat(date,'T',time), 'yyyyMMddTHHmmss'))
                    END AS 'date',
                    open,
                    high,
                    low,
                    close,
                    vol
                FROM %s
                WHERE""";

        if ("d".equals(type)) {
            sourceTable = "historical_raw_d";
            targetTable = "historical_d";
            query = String.format(historicalQuery, targetTable, ".US", sourceTable);
        } else if ("etf_d".equals(type)) {
            sourceTable = "historical_raw_etf_d";
            targetTable = "historical_etf_d";
            query = String.format(historicalQuery, targetTable, ".US", sourceTable);
        } else if ("indices_d".equals(type)) {
            sourceTable = "indices_raw_d";
            targetTable = "indices_d";
            query = String.format(historicalQuery, targetTable, "^", sourceTable);
        } else {
            return getFalseMap();
        }
        String latest = questDBService.getLatestDate(targetTable, null);
        System.out.println("Latest:" + latest);
        if (latest == null) {
            return getFalseMap();
        }
        query += " date > '" + latest + "' ORDER BY date, time ASC;";
        return questDBService.executeQuery(query);
    }

    public Map<String, Object> insertIntoIndicator52w(String type) {
        String sourceTable;
        String targetTable;
        if ("d".equals(type)) {
            sourceTable = "historical_d";
            targetTable = "indicator_d_52w";
        } else if ("etf_d".equals(type)) {
            sourceTable = "historical_etf_d";
            targetTable = "indicator_etf_52w";
        } else {
            return getFalseMap();
        }

        String latest = getLatestDate(targetTable, "GENERAL");
        System.out.println("Latest:" + latest);
        if (latest == null) {
            return getFalseMap();
        }
        String sourceCondition1 = "";
        String targetCondition2 = "";
        if (!"19710101".equals(latest)) {
            sourceCondition1 = "WHERE date > dateadd('d', -400, to_date('" + latest + "', 'yyyyMMdd'))";
            targetCondition2 = "WHERE date > to_date('" + latest + "', 'yyyyMMdd')";
        }

        String indicator52wQuery = """
                WITH first_stage AS
                (SELECT
                  'GENERAL' AS type,
                  date,
                  ticker,
                  high,
                  low,
                  close,
                  lag(close) OVER (
                      PARTITION BY ticker
                      ORDER BY date
                  ) AS 'previous_close',
                  vol,
                  lag(vol) OVER (
                      PARTITION BY ticker
                      ORDER BY date
                  ) AS 'previous_vol',
                  max(high) OVER (
                    PARTITION BY ticker
                      ORDER BY date
                      RANGE BETWEEN '365' DAY PRECEDING AND CURRENT ROW
                  ) AS 'high52w',
                  max(high) OVER (
                    PARTITION BY ticker
                      ORDER BY date
                      RANGE BETWEEN '365' DAY PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW
                  ) AS 'previous_high52w',
                  min(low) OVER (
                    PARTITION BY ticker
                      ORDER BY date
                      RANGE BETWEEN '365' DAY PRECEDING AND CURRENT ROW
                  ) AS 'low52w',
                  min(low) OVER (
                    PARTITION BY ticker
                      ORDER BY date
                      RANGE BETWEEN '365' DAY PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW
                  ) AS 'previous_low52w'
                FROM %s %s)
                INSERT INTO %s
                SELECT
                  type, date, ticker, high, low, close, previous_close, vol, previous_vol,
                  high52w, previous_high52w, (close - high52w)/high52w, low52w, previous_low52w, (close - low52w)/low52w
                FROM first_stage %s""";

        String query = String.format(indicator52wQuery, sourceTable, sourceCondition1, targetTable, targetCondition2);
        return questDBService.executeQuery(query);
    }

    public Map<String, Object> insertIntoIndicatorMA(String type, int interval, boolean truncate) {
        String sourceTable;
        String targetTable;
        if ("d".equals(type)) {
            sourceTable = "historical_d";
            targetTable = "indicator_d_MA";
        } else if ("etf_d".equals(type)) {
            sourceTable = "historical_etf_d";
            targetTable = "indicator_etf_MA";
        } else {
            return getFalseMap();
        }

        if (truncate) {
            boolean truncated = questDBService.truncateTable(targetTable);
            if (!truncated) {
                return getFalseMap();
            }
        }

        String indicatorMAQuery = """
                WITH first_stage AS
                (SELECT
                    date, ticker, close AS 'value1',
                    avg(close) OVER
                        (PARTITION BY ticker ORDER BY date
                        ROWS BETWEEN %s PRECEDING AND CURRENT ROW
                        EXCLUDE CURRENT ROW)
                    AS 'value2',
                    count() OVER
                        (PARTITION BY ticker ORDER BY date
                        ROWS BETWEEN UNBOUNDED PRECEDING AND %s PRECEDING)
                    AS 'total'
                FROM %s),
                second_stage AS
                (SELECT
                    date, ticker, value1, value2, total,
                    value1 - value2 AS 'difference',
                    ((value1 - value2) / value2) * 100 AS 'percentage',
                    first_value(value1 - value2) OVER\s
                        (PARTITION BY ticker ORDER BY date\s
                        ROWS 1 PRECEDING EXCLUDE CURRENT ROW)\s
                    AS 'previous_difference'
                FROM first_stage
                WHERE total > 0),
                third_stage AS
                (SELECT
                    date, ticker, value1, value2, total,
                    difference, previous_difference, percentage,
                    CASE
                        WHEN difference >=0 and previous_difference >= 0 THEN total
                        ELSE
                            CASE
                                WHEN difference < 0 and previous_difference < 0 THEN total
                                ELSE (1 - total)
                            END
                    END AS 'trend'
                FROM second_stage),
                fourth_stage AS
                (SELECT
                    date, ticker, value1, value2, total,
                    difference, previous_difference, percentage, trend,
                    min(trend) OVER (PARTITION BY ticker ORDER BY date\s
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                    AS 'minimum_trend'
                FROM third_stage)
                INSERT INTO %s
                SELECT
                    'MA_%s' AS type, date, ticker, value1, value2, total,
                    difference, previous_difference, percentage, trend, minimum_trend,
                    (total + minimum_trend) AS 'trending'
                FROM fourth_stage""";

        String query = String.format(indicatorMAQuery, interval, interval, sourceTable, targetTable, interval);
        return questDBService.executeQuery(query);
    }

    public Map<String, Object> insertIntoAnalysisMA(String indicatorType) {
        String condition;
        String query;
        if ("MA".equals(indicatorType)) {
            condition = " type LIKE 'MA_%'";
            query = """
                    INSERT INTO analysis_market
                    SELECT
                        type,
                        date,
                        COUNT(ticker) AS 'total',
                        SUM(CASE WHEN difference > 0 THEN 1 ELSE 0 END) AS 'count',
                        (SUM(CASE WHEN difference > 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(ticker)) * 100 AS 'percentage'
                    FROM
                      indicator_d_MA
                    WHERE
                    type LIKE 'MA_%'
                    AND total > 0""";
        } else {
            return getFalseMap();
        }
        String latest = questDBService.getLatestDate("analysis_market", condition);
        System.out.println("Latest:" + latest);
        if (latest == null) {
            return getFalseMap();
        }
        query += " AND date > to_date('" + latest + "', 'yyyyMMdd')";
        query += " GROUP BY type, date ORDER BY type, date ASC;";
        return questDBService.executeQuery(query);
    }

    public Map<String, Object> insertIntoIndicatorMACompare(String type, int firstInterval, int secondInterval) {
        String sourceTable;
        String targetTable;
        if ("d".equals(type)) {
            sourceTable = "indicator_d_MA";
            targetTable = "indicator_d_MA";
        } else if ("etf_d".equals(type)) {
            sourceTable = "historical_etf_d";
            targetTable = "indicator_etf_MA";
        } else {
            return getFalseMap();
        }

        String indicatorMAQuery = """
                WITH first_stage AS
                  (SELECT
                      i1.date, i1.ticker,
                      i1.value2 AS 'value1',
                      i2.value2 AS 'value2',
                          count() OVER\s
                          (PARTITION BY i2.ticker ORDER BY i2.date\s
                          )\s
                      AS 'total'
                  FROM %s i1
                  JOIN %s i2 ON i1.date = i2.date AND i1.ticker = i2.ticker
                  WHERE i1.type = 'MA_%s' and i2.type = 'MA_%s'),
                  second_stage AS
                  (SELECT
                      date, ticker, value1, value2, total,
                      value1 - value2 AS 'difference',
                      ((value1 - value2) / value2) * 100 AS 'percentage',
                      first_value(value1 - value2) OVER\s
                          (PARTITION BY ticker ORDER BY date
                          ROWS 1 PRECEDING EXCLUDE CURRENT ROW)
                      AS 'previous_difference'
                  FROM first_stage
                  WHERE total > 0),
                  third_stage AS
                  (SELECT
                      date, ticker, value1, value2, total,
                      difference, previous_difference, percentage,
                      CASE
                          WHEN difference >=0 and previous_difference >= 0 THEN total
                          ELSE
                              CASE
                                  WHEN difference < 0 and previous_difference < 0 THEN total
                                  ELSE (1 - total)
                              END
                      END AS 'trend'
                  FROM second_stage),
                  fourth_stage AS
                  (SELECT
                      date, ticker, value1, value2, total,
                      difference, previous_difference, percentage, trend,
                      min(trend) OVER (PARTITION BY ticker ORDER BY date\s
                          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                      AS 'minimum_trend'
                  FROM third_stage)
                  INSERT INTO %s
                  SELECT
                      '%s' AS type, date, ticker, value1, value2, total,
                      difference, previous_difference, percentage, trend, minimum_trend,
                      (total + minimum_trend) AS 'trending'
                  FROM fourth_stage""";
        String maType = String.format("MA_%s_%s", firstInterval, secondInterval);
        String query = String.format(indicatorMAQuery, sourceTable, sourceTable,
                firstInterval, secondInterval, targetTable, maType);
        return questDBService.executeQuery(query);
    }

    public Map<String, Object> updateAnalysisMA(String type, int firstInterval, int secondInterval) {
        int totalDuration = 0;
        Map<String, Object> result1 = insertIntoIndicatorMA(type, firstInterval, true);
        System.out.println("RESULT1:\n" + result1);
        if (!result1.containsKey("response")) {
            return getFalseMap();
        }
        totalDuration += getDuration(result1);
        Map<String, Object> response1 = (Map<String, Object>) result1.get("response");
        if (!"OK".equals(response1.getOrDefault("dml", "FAILURE"))) {
            return getFalseMap();
        }
        Map<String, Object> result2 = insertIntoIndicatorMA(type, secondInterval, false);
        System.out.println("RESULT2:\n" + result2);
        if (!result2.containsKey("response")) {
            return getFalseMap();
        }
        totalDuration += getDuration(result2);
        Map<String, Object> response2 = (Map<String, Object>) result1.get("response");
        if (!"OK".equals(response2.getOrDefault("dml", "FAILURE"))) {
            return getFalseMap();
        }
        Map<String, Object> result3 = insertIntoIndicatorMACompare(type, firstInterval, secondInterval);
        System.out.println("RESULT3:\n" + result3);
        if (!result3.containsKey("response")) {
            return getFalseMap();
        }
        totalDuration += getDuration(result3);
        Map<String, Object> response3 = (Map<String, Object>) result1.get("response");
        if (!"OK".equals(response3.getOrDefault("dml", "FAILURE"))) {
            return getFalseMap();
        }
        Map<String, Object> result4 = insertIntoAnalysisMA("MA");
        System.out.println("RESULT4:\n" + result4);
        if (!result4.containsKey("response")) {
            return getFalseMap();
        }
        totalDuration += getDuration(result4);
        Map<String, Object> map = getFalseMap();
        map.put("success", Boolean.TRUE);
        map.put("duration", totalDuration);
        return map;
    }

    public Map<String, Object> insertIntoAnalysis52w(String indicatorType) {
        String condition;
        String query;
        if ("high52w".equals(indicatorType)) {
            condition = " type = 'high52w'";
            query = """
                    INSERT INTO analysis_market
                    SELECT
                        'high52w' as 'type',
                        date,
                        count(ticker) as 'total',
                        SUM(CASE WHEN high52w > previous_high52w THEN 1 ELSE 0 END) AS 'count',
                        (SUM(CASE WHEN high52w > previous_high52w THEN 1 ELSE 0 END) * 1.0 / COUNT(ticker)) * 100 AS 'percentage'
                    FROM indicator_d_52w
                    WHERE
                    previous_close <> null""";
        } else if ("low52w".equals(indicatorType)) {
            condition = " type = 'low52w'";
            query = """
                    INSERT INTO analysis_market
                    SELECT
                        'low52w' as 'type',
                        date,
                        count(ticker) as 'total',
                        SUM(CASE WHEN low52w < previous_low52w THEN 1 ELSE 0 END) AS 'count',
                        (SUM(CASE WHEN low52w < previous_low52w THEN 1 ELSE 0 END) * 1.0 / COUNT(ticker)) * 100 AS 'percentage'
                    FROM indicator_d_52w
                    WHERE
                    previous_close <> null""";
        } else {
            return getFalseMap();
        }
        String latest = questDBService.getLatestDate("analysis_market", condition);
        System.out.println("Latest:" + latest);
        if (latest == null) {
            return getFalseMap();
        }
        query += " AND date > to_date('" + latest + "', 'yyyyMMdd')\n";
        query += " ORDER BY type, date ASC;";
        return questDBService.executeQuery(query);
    }

    public Map<String, Object> updateAnalysis52w(String type) {
        int totalDuration  = 0;
        Map<String, Object> result1 = insertIntoIndicator52w(type);
        System.out.println("RESULT1:\n" + result1);
        if (!result1.containsKey("response")) {
            return getFalseMap();
        }
        totalDuration += getDuration(result1);
        Map<String, Object> response1 = (Map<String, Object>) result1.get("response");
        if (!"OK".equals(response1.getOrDefault("dml", "FAILURE"))) {
            return getFalseMap();
        }
        Map<String, Object> result2 = insertIntoAnalysis52w("high52w");
        System.out.println("RESULT2:\n" + result2);
        if (!result2.containsKey("response")) {
            return getFalseMap();
        }
        totalDuration += getDuration(result2);
        Map<String, Object> response2 = (Map<String, Object>) result1.get("response");
        if (!"OK".equals(response2.getOrDefault("dml", "FAILURE"))) {
            return getFalseMap();
        }
        Map<String, Object> result3 = insertIntoAnalysis52w("low52w");
        System.out.println("RESULT3:\n" + result3);
        if (!result3.containsKey("response")) {
            return getFalseMap();
        }
        totalDuration += getDuration(result3);
        Map<String, Object> response3 = (Map<String, Object>) result1.get("response");
        if (!"OK".equals(response3.getOrDefault("dml", "FAILURE"))) {
            return getFalseMap();
        }
        Map<String, Object> map = getFalseMap();
        map.put("success", Boolean.TRUE);
        map.put("duration", totalDuration);
        return map;
    }

    private int getDuration(Map<String, Object> result) {
        Object durationObj = result.get("duration");
        if (durationObj instanceof Number) {
            return ((Number) durationObj).intValue();
        }
        return 0;
    }
}