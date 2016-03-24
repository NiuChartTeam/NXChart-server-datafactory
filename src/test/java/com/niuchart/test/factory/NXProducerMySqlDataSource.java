package com.niuchart.test.factory;

import com.niuchart.datafactory.NXDataProducer;
import org.apache.log4j.Logger;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Created by linxiaolong on 16/1/15.
 */
public abstract class NXProducerMySQLDataSource extends NXDataProducer {
    protected void initDataSet(String datasetId) throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        mDataSetConn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/dataTest?useCursorFetch=true&amp;useUnicode=false&amp;characterEncoding=UTF8","root","123456");
        mDataSetConn.setAutoCommit(false);
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
