package com.niuchart.test;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.niuchart.test.cube.NXCubeStructure;
import com.niuchart.test.factory.NXProducerSQLiteDataSource;
import com.niuchart.test.mock.NXDataSetMockSQLiteGenerator;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;
import java.util.UUID;
/**
 * Created by linxiaolong on 16/1/14.
 */
public class NXDataFactoryTest {
    private static final Logger logger = Logger.getLogger(NXDataFactoryTest.class);
    private static final String TARGET_PATH = "target/report/";
    private String mMetaDataId;
    NXCubeStructure mCube;
    @After
    public void restoreDataSet() {

    }
    /**
     * 模拟数据源，准备数据
     * @throws IOException
     */
    @Before
    public void prepareDataSet() throws Exception {
        //清理一下
        File path = new File(TARGET_PATH);
        if (path.exists()) {
            FileUtils.forceDelete(path);
        }
        //新的metaData id
        mMetaDataId = UUID.randomUUID().toString();
        //NXCubeStructure生成
        {
            InputStream inputStream = NXDataFactoryTest.class.getResourceAsStream("/metaData.json");
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            IOUtils.copy(inputStream, outputStream);
            String metaData = new String(outputStream.toByteArray());
            IOUtils.closeQuietly(inputStream);
            IOUtils.closeQuietly(outputStream);
            JSONObject metaObj = JSONObject.parseObject(metaData);
            mCube =NXCubeStructure.fill(metaObj);
        }
        //生成模拟数据
        {
            //从json中取出变成jsonArray
            InputStream inputStream = NXDataFactoryTest.class.getResourceAsStream("/example.json");
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            IOUtils.copy(inputStream, outputStream);
            String exampleString = new String(outputStream.toByteArray());
            IOUtils.closeQuietly(inputStream);
            IOUtils.closeQuietly(outputStream);
            JSONArray objects = JSONArray.parseArray(exampleString);
            //生成dataset
            NXDataSetMockSQLiteGenerator mock = new NXDataSetMockSQLiteGenerator();
            mock.createTable(mCube.getDataSetId(), mCube.getDimensions(), mCube.getMeasures());
            mock.insertData(objects);
            mock.commit();
            mock.close();
        }

    }

    @Test
    public void generateSQLiteFile() throws Exception {
        NXProducerSQLiteDataSource productor = new NXProducerSQLiteDataSource(){
            @Override
            public String getTargetDatabaseFolderPath() {
                return TARGET_PATH;
            }
        };
        productor.createCube(mMetaDataId, mCube.getDataSetId(), mCube.getDimensions(), mCube.getMeasures(), mCube.getEncrypt());
    }

    @After
    public void checkIsDataEncrypt() throws SQLException {
        File dbFile = new File(TARGET_PATH, "db.nxdb");
        String url = "jdbc:sqlite:" + dbFile.getAbsolutePath();

        Properties props = new Properties();
        final String password = mCube.getEncrypt();
        props.put("key", password);
        Connection conn = DriverManager.getConnection(url, props);
        conn.setAutoCommit(false);

        Statement st = conn.createStatement();
        st.executeUpdate("create table ants (col int)");
        st.executeUpdate("insert into ants values( 300 )");
        st.executeUpdate("insert into ants values( 400 )");
        st.close();
        conn.commit();
        conn.close();

        // Try reading without key.
        props.remove("key");
        conn = DriverManager.getConnection(url, props);

        try {
            st = conn.createStatement();
            ResultSet rs = st.executeQuery("select count(*) from ants");
            fail("Database not encrypted.");
        } catch (SQLException ignore) {
        }

        conn.close();
        props.put("key", password);
        conn = DriverManager.getConnection(url, props);

        st = conn.createStatement();
        ResultSet rs = st.executeQuery("select count(*) from ants");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        conn.close();

    }
}
