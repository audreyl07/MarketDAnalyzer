package dev.audreyl07.MDAnalyzer.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Business logic for retrieving and shaping market data for API responses.
 *
 * Responsibilities:
 * - Query QuestDB for stock and index historical series
 * - Transform raw rows into simplified structures (single vs full OHLCV)
 * - Provide market analysis series (52w highs/lows, MA 50/200 breadth)
 */
@Service
public class DataService {

    @Autowired
    QuestDBService questDBService;


    public List<Map<String, Object>> getData(String dataType, String resultType, String symbol) {
        String query;
        if ("stock".equals(dataType)) {
            query = "SELECT * FROM historical_d WHERE ticker = '" + symbol + "' ORDER BY date ASC;";
        } else if ("index".equals(dataType)) {
            query = "SELECT * FROM indices_d WHERE ticker = '" + symbol + "' ORDER BY date ASC;";
        } else {
            return List.of();
        }
        Map<String, Object> map = questDBService.executeQuery(query);
        Map<String, Object> response = (Map<String, Object>) map.get("response");
        List<Object> list = (List<Object>) response.get("dataset");

        if ("full".equalsIgnoreCase(resultType)) {
            return outputAsFull(list);
        }

        return outputAsSingle(list);
    }

    private List<Map<String, Object>> outputAsSingle(List<Object> list) {
        List<Map<String, Object>> listOfMap = new ArrayList<>();
        for (Object obj : list) {
            List<Object> row = (List<Object>) obj;
            Map<String, Object> m = new HashMap<>();
            m.put("time", convertToMillisecond(row.get(1)));
            m.put("value", row.get(5));
            m.put("volume", row.get(6));
            listOfMap.add(m);
        }
        System.out.println("Number of record:" + listOfMap.size());
        return listOfMap;
    }

    private List<Map<String, Object>> outputAsFull(List<Object> list) {
        List<Map<String, Object>> listOfMap = new ArrayList<>();
        for (Object obj : list) {
            List<Object> row = (List<Object>) obj;
            Map<String, Object> m = new HashMap<>();
            m.put("time", convertToMillisecond(row.get(1)));
            m.put("open", row.get(2));
            m.put("high", row.get(3));
            m.put("low", row.get(4));
            m.put("close", row.get(5));
            m.put("volume", row.get(6));
            listOfMap.add(m);
        }
        System.out.println("Number of record:" + listOfMap.size());
        return listOfMap;
    }

    private Long convertToMillisecond(Object object) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'");
        LocalDateTime dateTime = LocalDateTime.parse((String) object, formatter);
        long milliseconds = dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
        return milliseconds / 1000;
    }

    public List<Map<String, Object>> getAnalysis(String type) {
        String query;
        if ("high52w".equalsIgnoreCase(type)) {
            query = "SELECT date, percentage FROM analysis_market WHERE type = 'high52w' ORDER BY date ASC";
        } else if ("low52w".equalsIgnoreCase(type)) {
            query = "SELECT date, percentage FROM analysis_market WHERE type = 'low52w' ORDER BY date ASC";
        } else if ("ma_50_200".equalsIgnoreCase(type)) {
            query = "SELECT date, percentage FROM analysis_market WHERE type = 'MA_50_200' ORDER BY date ASC";
        } else {
            return List.of();
        }
        System.out.println("Query:" + query);
        Map<String, Object> map = questDBService.executeQuery(query);
        Map<String, Object> response = (Map<String, Object>) map.get("response");
        List<Map<String, Object>> list = (List<Map<String, Object>>) response.get("dataset");
        List<Map<String, Object>> listOfMap = new ArrayList<>();
        for (Object obj : list) {
            List<Object> row = (List<Object>) obj;
            Map<String, Object> m = new HashMap<>();
            m.put("time", convertToMillisecond(row.get(0)));
            m.put("value", row.get(1));
            listOfMap.add(m);
        }
        System.out.println("Number of record:" + listOfMap.size());
        return listOfMap;
    }

}