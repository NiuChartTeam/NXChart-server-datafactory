package com.niuchart.test.factory;

import com.niuchart.datafactory.NXDataProducer;
import org.apache.log4j.Logger;
import org.sqlite.SQLiteConfig;

import java.io.File;
import java.sql.*;

/**
 * Created by linxiaolong on 16/1/15.
 */
public abstract class NXProducerSQLiteDataSource extends NXDataProducer {
    private static final Logger logger = Logger.getLogger(NXProducerSQLiteDataSource.class);
    @Override
    protected void initDataSet(String datasetId) throws Exception {
        // dataSet文件是否存在
        File datasetDbFile = new File(getTargetDatabaseFolderPath()+ File.separator+ datasetId);
        if (!datasetDbFile.exists()) {
            throw new Exception("DataSet not found: " + datasetDbFile);
        }

        SQLiteConfig datasetConfig = new SQLiteConfig();
        datasetConfig.setReadOnly(true);
        mDataSetConn = DriverManager.getConnection("jdbc:sqlite:" + datasetDbFile.getAbsolutePath(), datasetConfig.toProperties());
        mDataSetConn.setReadOnly(true);
        mDataSetStmt = mDataSetConn.createStatement();
    }

    @Override
    public String getDataSetTableName() {
        //默认去读DATASET这张表达
        return super.getDataSetTableName();
    }

    @Override
    public String getCubeGenerateDbName() {
        //默认的名字db.nxdb
        return super.getCubeGenerateDbName();
    }
}
