# 数据库文件生成

数据库文件需要开发者自己在服务器端生成，生成的逻辑具体请参阅Java Maven工程, [NXChart-server-datafactory](https://github.com/NiuChartTeam/NXChart-server-datafactory)

其中metaData.json是多维的cube structure 参阅章节[`报表多维数据结构`](data_structure_report.md)，,需要根据您自身的数据源设定维粒度。

其中维度、粒度、度量的概念如下：

####Dimension-维度
维，人们观察数据的特定角度，是考虑问题时的一类属性，属性集合构成一个维（时间维、地理维等）。



####granularity-粒度

粒度，也称为维的层次，人们观察数据的某个特定角度（即某个维）还可以存在细节程度不同的各个描述方面（时间维：日期、月份、季度、年）。


####measure-度量

度量，多维数组的取值。（2009 年 12 月，北京，笔记本电脑，7898）。


以NXChart-server0datafactory中的[metaData.json](https://github.com/NiuChartTeam/NXChart-server-datafactory/blob/master/src/test/resources/metaData.json)为例。**encrypt**加上时会生成基于sqlcipher加密的sqlite.为null或""时不进行加密。

{%ace edit=false, lang='json', theme='monokai'%}  

    {
      "dataSetId": "exampleDataSetID.sqlite",
      "db": "example",
      "encrypt": "123456",
      "dimensions": [
        {
          "granularities": [
            {
              "key": "COL_0_0",
              "title": "市级行政"
            },
            {
              "key": "COL_0_1",
              "title": "区级行政"
            }
          ],
          "key": "Dimension_1",
          "title": "地区"
        },
        {
          "granularities": [
            {
              "key": "COL_0_2",
              "title": "年"
            },
            {
              "key": "COL_0_3",
              "title": "季"
            },
            {
              "key": "COL_0_4",
              "title": "月"
            },
            {
              "key": "COL_0_5",
              "title": "日"
            }
          ],
          "key": "Dimension_2",
          "title": "时间"
        }
      ],
      "measures": [
        {
          "key": "COL_0_6",
          "title": "一般公共预算支出"
        },
        {
          "key": "COL_0_8",
          "title": "一般公共服务支出"
        }
      ]
    }
    
        
{%endace%}   

此数据源结构中有2个维度：
* 时间-有4个粒度
    1. 年
    2. 季
    3. 月
    4. 日

* 地区-有2个粒度
    1. 市
    2. 区

另外2个度量:
* 一般公共预算支出
* 一般公共服务支出
 
此结构的cube元数据应如[example.json](https://github.com/NiuChartTeam/NXChart-server-datafactory/blob/master/src/test/resources/example.json)所示一样：

| 年 | 季 | 月 | 日 | 市 | 区 | 预算支出 | 服务支出 |
| -- | -- | -- | -- | -- | -- | -- | -- |
| 2015 | Q3| 7月 | 15日| 宁波 | 鄞州区 | 7836 | 6281 |
| 2015 | Q3 |7月 | 15日 |宁波| 象山县 | 1964 | 3308 |
...

如果数据源是SQLite，生成层级DB代码请参照[NXDataFactorySQLiteDataSourceTest](https://github.com/NiuChartTeam/NXChart-server-datafactory/blob/master/src/test/java/com/niuchart/test/NXDataFactorySQLiteDataSourceTest.java)。


如果数据源是SQLite，生成层级DB代码请参照[NXDataFactoryMySQLDataSourceTest](https://github.com/NiuChartTeam/NXChart-server-datafactory/blob/master/src/test/java/com/niuchart/test/NXDataFactoryMySQLDataSourceTest.java)。
[NXDataSetMockMySQLGenerator](https://github.com/NiuChartTeam/NXChart-server-datafactory/blob/master/src/test/java/com/niuchart/test/mock/NXDataSetMockMySQLGenerator.java)是用来生成数据模拟MySQL数据源的。

执行jUnitTest之后会有target中生成db.nxdb即是所需的数据库文件。
[NXDataSetMockSQLiteGenerator](https://github.com/NiuChartTeam/NXChart-server-datafactory/blob/master/src/test/java/com/niuchart/test/mock/NXDataSetMockSQLiteGenerator.java)是用来生成数据模拟SQlite数据源的。

SQLite数据源生成层级数据库代码：
```java

public void generateSQLiteFile() throws Exception {
    NXProducerSQLiteDataSource productor = new NXProducerSQLiteDataSource(){
        @Override
        public String getTargetDatabaseFolderPath() {
            return TARGET_PATH;
        }
    };
    productor.createCube(mMetaDataId, mCube.getDataSetId(), mCube.getDimensions(), mCube.getMeasures(), mCube.getEncrypt());
}
        
``` 

MySQL数据源生成层级数据库代码：

```java

public void generateSQLiteFile() throws Exception {
  //生成取终供sdk使用的层级数据库，默认放在target目录
  NXProducerMySQLDataSource productor = new NXProducerMySQLDataSource(){
      @Override
      public String getTargetDatabaseFolderPath() {
          return TARGET_PATH;
      }
  };
  productor.createCube(mMetaDataId, mCube.getDataSetId(), mCube.getDimensions(), mCube.getMeasures(), mCube.getEncrypt());
}

```   

