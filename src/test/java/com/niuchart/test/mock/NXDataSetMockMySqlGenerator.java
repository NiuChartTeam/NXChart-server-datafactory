package com.niuchart.test.mock;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.niuchart.datafactory.command.NXKeyCommand;
import com.niuchart.datafactory.command.NXKeyDimenCommand;
import com.niuchart.datafactory.command.NXKeyMeasureCommand;
import org.apache.log4j.Logger;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by linxiaolong on 16/1/15.
 */
public class NXDataSetMockMySQLGenerator {
    private static final Logger logger = Logger.getLogger(NXDataSetMockSQLiteGenerator.class);
    private Connection conn;
    private Statement stmt;
    private PreparedStatement pareStmt;

    private List<String> mAllKeys = new ArrayList<String>();

    public String getDataSetTableName() {
        //默认去读DATASET这张表达
        return "DATASET";
    }
    public String getMySQLConnectionUrl() {
        return "jdbc:mysql://127.0.0.1:3306/dataTest?useCursorFetch=true&amp;useUnicode=false&amp;characterEncoding=UTF8";
    }
    public String getMySQLConnectionUserName() {
        return "root";
    }
    public String getMySQLConnectionPassword() {
        return "123456";
    }


    public void createTable(String datasetId,  List<? extends NXKeyDimenCommand> dimensions, List<? extends NXKeyMeasureCommand> measures) throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        conn = DriverManager.getConnection(getMySQLConnectionUrl(), getMySQLConnectionUserName(), getMySQLConnectionPassword());
        conn.setAutoCommit(false);
        stmt = conn.createStatement();

        stmt.execute(String.format("DROP TABLE IF EXISTS `%s`", getDataSetTableName()));

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(getDataSetTableName()).append("(");


        for (int i = 0; i < dimensions.size(); i++ ) {
            NXKeyDimenCommand dimenCommand = dimensions.get(i);
            for (int j = 0; j < dimenCommand.getGranularities().size(); j++) {
                NXKeyCommand gran = dimenCommand.getGranularities().get(j);
                if (i != 0 || j != 0) {
                    sb.append(",");
                }
                mAllKeys.add(gran.getKey());
                sb.append(gran.getKey()).append(" ");
                sb.append("VARCHAR(255)");
            }
        }
        for (int i = 0; i < measures.size(); i++) {
            sb.append(",");
            NXKeyMeasureCommand measure = measures.get(i);
            mAllKeys.add(measure.getKey());
            sb.append(measure.getKey()).append(" ");
            sb.append("DOUBLE");
        }

        sb.append(", _ID INTEGER PRIMARY KEY AUTO_INCREMENT");

        sb.append(")");

        logger.info("SQL for creating metadata dataset table: " + sb.toString());
        stmt.execute(sb.toString());
        createPreparestatementInsert();
    }

    protected void createPreparestatementInsert() throws Exception {

        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(getDataSetTableName()).append("(");
        for (int m = 0; m < mAllKeys.size(); m++) {
            if (m > 0) {
                sb.append(",");
            }
            sb.append(mAllKeys.get(m));
        }
        sb.append(")");

        sb.append(" values(");
        for (int m = 0; m < mAllKeys.size(); m++) {
            if (m > 0) {
                sb.append(",");
            }
            sb.append("?");

        }
        sb.append(")");
        logger.debug("SQL for insert metadata dataset row: " + sb.toString());
        pareStmt = conn.prepareStatement(sb.toString());

    }

    public void insertData(JSONArray allData) throws Exception {
        for (int i = 0; i < allData.size(); i++) {
            JSONObject row = allData.getJSONObject(i);
            for (int j = 0; j < mAllKeys.size(); j++) {
                String string = row.getString(mAllKeys.get(j));
                pareStmt.setString(j+1 , string);
            }
            pareStmt.addBatch();
        }
        pareStmt.executeBatch();
    }

    public void commit() throws Exception {
        pareStmt.executeBatch();
        conn.commit();
    }

    public void close() {
        try {
            pareStmt.close();
        } catch (Exception e) {
        }
        try {
            stmt.close();
        } catch (Exception e) {
        }
        try {
            conn.close();
        } catch (Exception e) {
        }
    }
}
