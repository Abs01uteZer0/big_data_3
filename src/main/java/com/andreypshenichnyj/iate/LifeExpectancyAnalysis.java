package com.andreypshenichnyj.iate;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.data.category.DefaultCategoryDataset;

import javax.swing.*;

public class LifeExpectancyAnalysis {

    public static void main(String[] args) {
        // Создаем SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Life Expectancy Analysis")
                .master("local[*]") // Используем многопоточный режим на одной машине
                .getOrCreate();

        // Определяем схему для данных
        StructType schema = new StructType()
                .add("Year", DataTypes.IntegerType, false)
                .add("Race", DataTypes.StringType, false)
                .add("Sex", DataTypes.StringType, false)
                .add("Average Life Expectancy (Years)", DataTypes.DoubleType, true)
                .add("Age-adjusted Death Rate", DataTypes.DoubleType, false);

        // Загружаем CSV файл
        Dataset<Row> data = spark.read()
                .option("header", "true")
                .schema(schema)
                .csv("src/main/resources/data.csv");

        // Регистрируем временную таблицу для SQL запросов
        data.createOrReplaceTempView("life_data");

        // 0. Выводим csv на экран
        Dataset<Row> showAll = spark.sql(
                "SELECT * " +
                        "FROM life_data;"
        );
        showAll.show();

        // 1. Средняя продолжительность жизни по годам
        Dataset<Row> avgLifeExpectancyByYear = spark.sql(
                "SELECT Year, AVG(`Average Life Expectancy (Years)`) AS AvgLifeExpectancy " +
                        "FROM life_data " +
                        "GROUP BY Year " +
                        "ORDER BY Year"
        );
        avgLifeExpectancyByYear.show();

        // Построение графика средней продолжительности жизни по годам
        DefaultCategoryDataset dataset1 = new DefaultCategoryDataset();
        avgLifeExpectancyByYear.collectAsList().forEach(row -> {
            int year = row.getInt(0);
            double avgLifeExpectancy = row.getDouble(1);
            dataset1.addValue(avgLifeExpectancy, "Average Life Expectancy", Integer.toString(year));
        });

        JFreeChart lineChart1 = ChartFactory.createLineChart(
                "Average Life Expectancy by Year",
                "Year",
                "Average Life Expectancy (Years)",
                dataset1
        );

        displayChart(lineChart1, "Average Life Expectancy by Year");

        // 2. Средний уровень смертности по расам
        Dataset<Row> avgDeathRateByRace = spark.sql(
                "SELECT Race, AVG(`Age-adjusted Death Rate`) AS AvgDeathRate " +
                        "FROM life_data " +
                        "GROUP BY Race " +
                        "ORDER BY AvgDeathRate DESC"
        );
        avgDeathRateByRace.show();

        DefaultCategoryDataset dataset2 = new DefaultCategoryDataset();
        avgDeathRateByRace.collectAsList().forEach(row -> {
            String race = row.getString(0);
            double avgDeathRate = row.getDouble(1);
            dataset2.addValue(avgDeathRate, "Average Death Rate", race);
        });

        JFreeChart barChart = ChartFactory.createBarChart(
                "Average Death Rate by Race",
                "Race",
                "Average Death Rate",
                dataset2
        );

        displayChart(barChart, "Average Death Rate by Race");

        // 3. Сравнение средней продолжительности жизни между полами
        Dataset<Row> avgLifeExpectancyBySex = spark.sql(
                "SELECT Sex, AVG(`Average Life Expectancy (Years)`) AS AvgLifeExpectancy " +
                        "FROM life_data " +
                        "GROUP BY Sex " +
                        "ORDER BY Sex"
        );
        avgLifeExpectancyBySex.show();

        DefaultCategoryDataset dataset3 = new DefaultCategoryDataset();
        avgLifeExpectancyBySex.collectAsList().forEach(row -> {
            String sex = row.getString(0);
            double avgLifeExpectancy = row.getDouble(1);
            dataset3.addValue(avgLifeExpectancy, "Average Life Expectancy", sex);
        });

        JFreeChart barChartSex = ChartFactory.createBarChart(
                "Average Life Expectancy by Sex",
                "Sex",
                "Average Life Expectancy",
                dataset3
        );

        displayChart(barChartSex, "Average Life Expectancy by Sex");

        // 4. Изменение смертности по годам для каждой расы
        Dataset<Row> deathRateByYearAndRace = spark.sql(
                "SELECT Year, Race, AVG(`Age-adjusted Death Rate`) AS AvgDeathRate " +
                        "FROM life_data " +
                        "GROUP BY Year, Race " +
                        "ORDER BY Year, Race"
        );
        deathRateByYearAndRace.show();

        DefaultCategoryDataset dataset4 = new DefaultCategoryDataset();
        deathRateByYearAndRace.collectAsList().forEach(row -> {
            int year = row.getInt(0);
            String race = row.getString(1);
            double avgDeathRate = row.getDouble(2);
            dataset4.addValue(avgDeathRate, race, Integer.toString(year));
        });

        JFreeChart lineChart2 = ChartFactory.createLineChart(
                "Death Rate by Year and Race",
                "Year",
                "Average Death Rate",
                dataset4
        );

        displayChart(lineChart2, "Death Rate by Year and Race");

        // 5. Корреляция между продолжительностью жизни и уровнем смертности
        Dataset<Row> correlation = spark.sql(
                "SELECT corr(`Average Life Expectancy (Years)`, `Age-adjusted Death Rate`) AS Correlation " +
                        "FROM life_data"
        );
        correlation.show();

        // Завершение работы
        spark.stop();
    }

    private static void displayChart(JFreeChart chart, String title) {
        SwingUtilities.invokeLater(() -> {
            JFrame frame = new JFrame(title);
            frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
            frame.add(new ChartPanel(chart));
            frame.pack();
            frame.setVisible(true);
        });
    }
}