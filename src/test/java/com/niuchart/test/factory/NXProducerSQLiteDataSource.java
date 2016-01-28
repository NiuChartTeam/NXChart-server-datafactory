package com.niuchart.test.factory;

import com.niuchart.datafactory.MetadataCubeOperator;
import com.niuchart.datafactory.NXDataProducer;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.sqlite.SQLiteConfig;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by linxiaolong on 16/1/15.
 */
public abstract class NXProducerSQLiteDataSource extends NXDataProducer {
    private static final Logger logger = Logger.getLogger(NXProducerSQLiteDataSource.class);
    private Connection datasetConn;
    private Statement datasetStmt;


    @Override
    public void close() {
        super.close();
        try {
            datasetStmt.close();
        } catch (Exception e) {
        }
        try {
            datasetConn.close();
        } catch (Exception e) {
        }
    }

    @Override
    protected void initDataSet(String datasetId) throws Exception {
        // dataSet文件是否存在
        File datasetDbFile = new File(getTargetDatabaseFolderPath()+ File.separator+ datasetId);
        if (!datasetDbFile.exists()) {
            throw new Exception("DataSet not found: " + datasetDbFile);
        }

        SQLiteConfig datasetConfig = new SQLiteConfig();
        datasetConfig.setReadOnly(true);
        datasetConn = DriverManager.getConnection("jdbc:sqlite:" + datasetDbFile.getAbsolutePath(), datasetConfig.toProperties());
        datasetConn.setReadOnly(true);
        datasetStmt = datasetConn.createStatement();
    }

    @Override
    protected List<Object> getDataSetDistinctValueByGranKey(String granKey) throws Exception {
        List<Object> result = new ArrayList<Object>();
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("select distinct ").append(granKey).append(", min(_id)");
        sqlBuilder.append(" from ").append(TABLE_NAME).append(" group by ").append(granKey).append(" order by min(_id)");
        String sql = sqlBuilder.toString();
        logger.info("Query sql for column table: " + sql);
        ResultSet rs = datasetStmt.executeQuery(sql);

        List<String> columnNames = new ArrayList<String>();
        columnNames.add(granKey);
        while (rs.next()) {
            Object objValue = rs.getObject(granKey);
            result.add(objValue);
        }
        return result;
    }

    @Override
    protected List<Map<String, Object>> queryLevelDataFromDataSet( List<String> selectFields,List<String> groupFields,
                                                                   List<String> dimensionColumnNames,List<String> measureColumnNames,boolean isGroupNotNeeded) throws SQLException {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("select ").append(StringUtils.join(selectFields, ","));

        sqlBuilder.append(" from ").append(TABLE_NAME);
        if (!isGroupNotNeeded) {
            sqlBuilder.append(" group by ").append(StringUtils.join(groupFields, ","));
        }

        String sql = sqlBuilder.toString();

        logger.info("Query sql for table: " + sql);


        ResultSet rs = datasetStmt.executeQuery(sql);

        List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
        while (rs.next()) {
            Map<String, Object> eachObj = new HashMap<String, Object>();
            for (String columnName : dimensionColumnNames) {
                eachObj.put(columnName, rs.getString(columnName));
            }

            for (String columnName : measureColumnNames) {
                eachObj.put(columnName, rs.getDouble(columnName));
            }
            results.add(eachObj);
        }
        return results;
    }
    abstract public String getTargetDatabaseFolderPath();
}
