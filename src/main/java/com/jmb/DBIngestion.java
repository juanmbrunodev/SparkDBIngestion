package com.jmb;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DBIngestion {

    private static final Logger LOGGER = LoggerFactory.getLogger(DBIngestion.class);

    public static void main(String[] args) throws Exception {

        LOGGER.info("Application starting up");
        DBIngestion app = new DBIngestion();
        app.init();
        LOGGER.info("Application gracefully exiting...");
    }

    private void init() throws Exception {

        SparkConf appConfig = new SparkConf().set("spark.testing.memory", "900000000");

        //Create the Spark Session
        SparkSession session = SparkSession.builder()
                .appName("SparkFileFormats")
                .config(appConfig)
                .master("local").getOrCreate();

    }

}
