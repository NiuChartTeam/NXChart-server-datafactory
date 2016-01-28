/**
 * 
 */
package com.niuchart.datafactory;

import java.util.List;
import java.util.Map;

import com.niuchart.datafactory.command.*;

/**
 * @author linxiaolong
 * 
 */
public interface MetadataCubeOperator {
	
	String DB_FILE_NAME = "db.nxdb";
	String TABLE_NAME = "DATASET";

	/**
	 * 取粒度的所有粒度值
	 * @param metadataId    元数据的id
	 * @param cubePath		cube数据的文件绝对路径
	 * @param columns		粒度值的key集合
	 * @return	返回指定粒度的所有粒度值映射
	 * @throws Exception
	 */
	Map<String, List<String>> getColumnValues(String metadataId, String cubePath, List<String> columns) throws Exception;

	/**
	 * 根据dataset创建cube
	 */
	String createCube(String metadataId, String datasetId, List<? extends NXKeyDimenCommand> dimensions, List<? extends NXKeyMeasureCommand> measures,String password) throws Exception;
	
	/**
	 * 清除数据
	 */
	void closeAndClean();

	/**
	 * 关闭
	 */
	void close();
	
	/**
	 * 删除相关资源
	 */
	void clean(String metadataId, String cubeId) throws Exception;

}
