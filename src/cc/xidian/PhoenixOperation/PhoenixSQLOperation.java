package cc.xidian.PhoenixOperation;

import cc.xidian.GeoHash.GeoHashConversion;
import cc.xidian.GeoObject.GeoPointTableRecord;
import cc.xidian.GeoObject.RectangleQueryScope;
import cc.xidian.geoUtil.FileUtil;
import cc.xidian.geoUtil.RandomOperation;
import cc.xidian.geoUtil.RandomString;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

/**
 * Created by hadoop on 2016/9/8.
 */
public class PhoenixSQLOperation {
    public static Connection conn;
    public static Statement stmt;
    //public static int count;

    public static void createAndInsertRecordToTableNamedGeoPointTable(){
        //1、创建表操作
        String sqlCreateTableNamedGeoPointTable = "Create Table if not exists GeoPointTableLWD1MGS("
                +"geoID    Integer not null Primary key,"
                +"geoName    varchar(32),"
                +"xLongitude    double,"
                +"yLatitude    double,"
                +"geoHashValue    varchar(32),"
                +"geoHashValueLong    bigint "
                +")";
        long startTimeCreateTable = System.currentTimeMillis();
        createTable(sqlCreateTableNamedGeoPointTable);//创建表操作
        long endTimeCreateTable = System.currentTimeMillis();
        //2、插入数据操作
        String sqlInsert = "upsert into GeoPointTableLWD1MGS values(?,?,?,?,?,?)";
        long startTimeInsertRecord = System.currentTimeMillis();
        insertRecordToTableNamedGeoPointTable(sqlInsert);
        long endTimeInsertRecord  = System.currentTimeMillis();
        //3、查询操作
        String sqlSelect = "select * from GeoPointTableLWD1MGS where geoID > 999000";
        long startTimeSelectHaveResults = System.currentTimeMillis();
        selectHaveResults(sqlSelect);
        long endTimeSelectHaveResults = System.currentTimeMillis();
        //4、输出信息
        System.out.println("GeoPointTableLWD-CreateTable-Time: "+(endTimeCreateTable - startTimeCreateTable));
        System.out.println("GeoPointTableLWD-InsertRecord-Time: "+(endTimeInsertRecord - startTimeCreateTable));
        System.out.println("GeoPointTableLWD-SelectOption1-Time: "+(endTimeSelectHaveResults - startTimeSelectHaveResults));
    }
    public static void createAndInsertRecordToTableNamedGeoPointTableString(){
        //1、创建表操作
        String sqlCreateTableNamedGeoPointTable = "Create Table if not exists GeoPointTable1MGeoHashLong60("
                +"geoID    Integer not null Primary key,"
                +"geoName    varchar(32),"
                +"xLongitude    double,"
                +"yLatitude    double,"
                +"geoHashValue    varchar(32),"
                +"geoHashValueLong    bigint "
                +")";
        long startTimeCreateTable = System.currentTimeMillis();
        createTable(sqlCreateTableNamedGeoPointTable);//创建表操作
        long endTimeCreateTable = System.currentTimeMillis();
        //2、插入数据操作
        String sqlInsert = "upsert into GeoPointTable1MGeoHashLong60 values(?,?,?,?,?,?)";
        long startTimeInsertRecord = System.currentTimeMillis();
        insertRecordToTableNamedGeoPointTable(sqlInsert);
        long endTimeInsertRecord  = System.currentTimeMillis();
        //3、查询操作
        String sqlSelect = "select * from GeoPointTable1MGeoHashLong60 where geoID > 999000";
        long startTimeSelectHaveResults = System.currentTimeMillis();
        selectHaveResults(sqlSelect);
        long endTimeSelectHaveResults = System.currentTimeMillis();
        //4、输出信息
        System.out.println("GeoPointTableLWD-CreateTable-Time: "+(endTimeCreateTable - startTimeCreateTable));
        System.out.println("GeoPointTableLWD-InsertRecord-Time: "+(endTimeInsertRecord - startTimeCreateTable));
        System.out.println("GeoPointTableLWD-SelectOption1-Time: "+(endTimeSelectHaveResults - startTimeSelectHaveResults));
    }
    /**
     * 删除表操作很少执行
     */
    public static void dropTableNamedGeoPointTable(){
        String sqlDropTableNamedGeoPointTable = "Drop Table GeoPointTable1MGeoHashString";
        try {
            stmt.executeUpdate(sqlDropTableNamedGeoPointTable);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
    /**
     * 函数功能：连接数据存储系统的操作，此处为Phoenix连接HBase的操作
     */
    public static void getConnectionWithHBase() {
        //以下两行为UDF函数使用配置
        Properties propsUDF = new Properties();
        propsUDF.setProperty("phoenix.functions.allowUserDefinedFunctions", "true");
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");// 加载Mysql数据驱动
            String url = "jdbc:phoenix:cloudgis1.com:2181:/hbase-unsecure";//phoenix连接URL
            conn = DriverManager.getConnection(url,propsUDF);// 创建数据连接
            conn.setAutoCommit(false);
            stmt = conn.createStatement();
            //System.out.println("Successful!");
        } catch (Exception e) {
            System.out.println("数据库连接失败" + e.getMessage());
        }
    }
    /**
     * 函数功能：断开与数据存储系统的连接，此处为断开Phoenix与HBase的连接
     */
    public static void closeConnectionWithHBase(){
        try {
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    /**
     * 函数功能：删除表操作，通用
     * @param sqlDropTable 删除表的SQL语句
     */
    public static void dropTable(String sqlDropTable){
        try {
            stmt.executeUpdate(sqlDropTable);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    /**
     * 函数功能：创建表操作，通用
     * @param sqlCreateTable 创建表的SQL语句
     */
    public static void createTable(String sqlCreateTable){
        try {

            stmt.executeUpdate(sqlCreateTable);//执行创建表的SQL语句即可
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void insertRecordToTableNamedGeoPointTable(String sqlInsertRecordToTableNamedGeoPointTable){
        try {
            PreparedStatement pst = conn.prepareStatement(sqlInsertRecordToTableNamedGeoPointTable);
            //每10万行记录作为一个插入单元，共插入100次，总共插入1亿条记录
            for(int j=0;j<10;j++){
                for(int i=0;i<100000;i++){
                    pst.setInt(1,(i+100000*j));
                    pst.setString(2, RandomOperation.RandomStringSimple(6));
                    double xLongitudeTemp = RandomOperation.RandomDouble(-180.0, 180.0);
                    pst.setDouble(3,xLongitudeTemp );
                    double yLatitudeTemp = RandomOperation.RandomDouble(-90.0, 90.0);
                    pst.setDouble(4, yLatitudeTemp);
                    String geoHashValueTemp = GeoHashConversion.encodeGeoHashFromLonAndLat(xLongitudeTemp, yLatitudeTemp);
                    pst.setString(5, geoHashValueTemp);
                    long geoHashValueLongTemp = GeoHashConversion.encodeGeoHashFromLonAndLatLong(xLongitudeTemp, yLatitudeTemp);//使用张洋的方式求long类型的GeoHash编码值
                    //String strBinaryGeoHashTemp = GeoHashConversion.encodeGeoHashFromLonAndLatBinaryString(xLongitudeTemp,yLatitudeTemp);
                    pst.setLong(6, geoHashValueLongTemp);
                    pst.addBatch();
                }
                pst.executeBatch();
                conn.commit();
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    /**
     * 函数功能：无输出结果的查询操作，不通用
     * @param sqlSelect 执行查询的SQL语句
     */
    public static void selectNoResults(String sqlSelect){
        ResultSet rs;
        try {
            rs = stmt.executeQuery(sqlSelect);
            //此处判断不能用rs.next()，否则最终得到的结果集会少一个
            if(rs.wasNull()){
                System.out.println("未找到记录，查询出错！");
                System.exit(-1);
            }
            while(rs.next()){
                int geoID = rs.getInt("geoID");
                String geoName = rs.getString("geoName");
                double xLongitude = rs.getDouble("xLongitude");
                double yLatitude = rs.getDouble("yLatitude");
                String geoHashValue = rs.getString("geoHashValue");
                long geoHashValueLong = rs.getLong("geoHashValueLong");
                GeoPointTableRecord g = new GeoPointTableRecord(geoID,geoName,xLongitude,yLatitude,geoHashValue,geoHashValueLong);
                //String password = rs.getString("password");
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }//执行SQL语句并保存结果

    }
    /**
     * 函数功能：有输出结果的查询操作，不通用
     * @param sqlSelect 执行查询的SQL语句
     */
    public static void selectHaveResults(String sqlSelect){
        ResultSet rs;
        try {
            rs = stmt.executeQuery(sqlSelect);
            //此处判断不能用rs.next()，否则最终得到的结果集会少一个
            if(rs.wasNull()){
                System.out.println("未找到记录，查询出错！");
                System.exit(-1);
            }
            while(rs.next()){
                int geoID = rs.getInt("geoID");
                String geoName = rs.getString("geoName");
                double xLongitude = rs.getDouble("xLongitude");
                double yLatitude = rs.getDouble("yLatitude");
                String geoHashValue = rs.getString("geoHashValue");
                long geoHashValueLong = rs.getLong("geoHashValueLong");
                GeoPointTableRecord g = new GeoPointTableRecord(geoID,geoName,xLongitude,yLatitude,geoHashValue,geoHashValueLong);
                System.out.println(g.toString());
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }//执行SQL语句并保存结果
    }
    /**
     * 函数功能：有输出结果的查询操作，并将输出结果集写入文件不通用
     */
    public static void selectResultsToFile()throws Exception{
        ResultSet rs;
        String sqlSelect = "select * from GeoPointTableLWD where geoID >9990000";
        File fileGeoPointTableLWD = new File("GeoPointTableLWD.txt");
        try {
            rs = stmt.executeQuery(sqlSelect);
            if(rs.wasNull()){
                System.out.println("未找到记录，查询出错！");
                System.exit(-1);
            }
            while(rs.next()){
                int geoID = rs.getInt("geoID");
                String geoName = rs.getString("geoName");
                double xLongitude = rs.getDouble("xLongitude");
                double yLatitude = rs.getDouble("yLatitude");
                String geoHashValue = rs.getString("geoHashValue");
                long geoHashValueLong = rs.getLong("geoHashValueLong");
                GeoPointTableRecord g = new GeoPointTableRecord(geoID,geoName,xLongitude,yLatitude,geoHashValue,geoHashValueLong);
                FileUtil.writeGeoPointTableRecordsToFile(fileGeoPointTableLWD,g.toString());
                //System.out.println(g.toString());
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }//执行SQL语句并保存结果
    }


    /**
     * 函数功能：Query0-无geoHash索引的范围查询，遍历方式，全表扫描，将所有记录集读到本地内存，然后分别进行判断，这种方式效率非常低，复杂度为O(n)，只用于对比查询的结果的正确性
     * @param rQS 查询范围
     */
    public static ArrayList<GeoPointTableRecord> selectAndQueryRecordsWithoutGeoHashIndex(RectangleQueryScope rQS){
        ResultSet rs;
        String sqlSelectQuery = "select * from GeoPointTableLWD1MLong";
        ArrayList<GeoPointTableRecord> geoPointTableRecords = new ArrayList<GeoPointTableRecord>();
        try {
            rs = stmt.executeQuery(sqlSelectQuery);
            //此处判断不能用rs.next()，否则最终得到的结果集会少一个
            if(rs.wasNull()){
                System.out.println("未找到记录，查询出错！");
                System.exit(-1);
            }
            //int count = 0;
            while(rs.next()){
                //count++;
                int geoID = rs.getInt("geoID");
                String geoName = rs.getString("geoName");
                double xLongitude = rs.getDouble("xLongitude");
                double yLatitude = rs.getDouble("yLatitude");
                String geoHashValue = rs.getString("geoHashValue");
                long geoHashValueLong = rs.getLong("geoHashValueLong");
                GeoPointTableRecord g = new GeoPointTableRecord(geoID,geoName,xLongitude,yLatitude,geoHashValue,geoHashValueLong);
                //本地内存过滤
                if(rQS.isContainGeoPointTableRecord(g)){
                    geoPointTableRecords.add(g);
                }
            }
            //System.out.println("-------------------"+count+"=======================");
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }//执行SQL语句并保存结果
        return geoPointTableRecords;
    }

    /**
     * 函数功能：Query1-SQL+直接判断，未建geoHash索引，使用SQL语句在服务器端进行执行判断，也是遍历方式，全表扫描，但比本地遍历查询要快得多
     * 优点：与本地内存遍历相比，在服务器端执行SQL判断语句，效率高；查询结果必然正确
     * @param rQS 矩形查询范围对象
     * @return 矩形查询后的记录
     */
    public static ArrayList<GeoPointTableRecord> selectAndQueryRecordsWithDirectJudge(RectangleQueryScope rQS){
        ResultSet rs;//查询结果集
        ArrayList<GeoPointTableRecord> geoPointTableRecords = new ArrayList<GeoPointTableRecord>();//保存查询结果记录的数组
        //RectangleQuery1:直接判断的SQL语句字符串
        String sqlDirectJudgeQuery = "select * from geoPointTableLWD1MGS where " +
                "xLongitude>="+rQS.xLongitudeBL+" and "+"xLongitude <= "+rQS.xLongitudeTR
                + " and "+"yLatitude >= "+rQS.yLatitudeBL+" and "+"yLatitude <= "+rQS.yLatitudeTR;
        try {
            rs = stmt.executeQuery(sqlDirectJudgeQuery);
            //此处判断不能用rs.next()，否则最终得到的结果集会少一个
            if(rs.wasNull()){
                System.out.println("未找到记录，查询出错！");
                System.exit(-1);
            }
            while(rs.next()){
                int geoID = rs.getInt("geoID");
                String geoName = rs.getString("geoName");
                double xLongitude = rs.getDouble("xLongitude");
                double yLatitude = rs.getDouble("yLatitude");
                String geoHashValue = rs.getString("geoHashValue");
                long geoHashValueLong = rs.getLong("geoHashValueLong");
                GeoPointTableRecord g = new GeoPointTableRecord(geoID,geoName,xLongitude,yLatitude,geoHashValue,geoHashValueLong);
                geoPointTableRecords.add(g);
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }//执行SQL语句并保存结果
        return geoPointTableRecords;
    }
    /**
     * 函数功能：Query2：SQL+UDF函数，未建geoHash索引，使用带UDF约束的SQL语句在服务器端进行查询
     * 评价：和执行SQL直接判断语句本质一样，但比直接判断方式要慢一下，本查询是为了测试UDF函数是否可用
     * @param rQS 矩形查询范围对
     * @return 矩形查询后的记录
     */
    public static ArrayList<GeoPointTableRecord> selectAndQueryRecordsWithUDFFunction(RectangleQueryScope rQS){
        ResultSet rs;//查询结果集
        ArrayList<GeoPointTableRecord> geoPointTableRecords = new ArrayList<GeoPointTableRecord>();//保存查询结果记录的数组
        String sqlDropRectangleQueryUDFFunction = "DROP FUNCTION IF EXISTS RectangleQueryUDFFunction";//删除UDF函数的SQL语句
        String sqlCreateRectangleQueryUDFFunction = "CREATE FUNCTION RectangleQueryUDFFunction(Double, Double, Varchar) " +
                "returns Integer as 'cc.xidian.PhoenixOperation.UDFRectangleDemoLWD' " +
                "using jar 'hdfs://cloudgis/apps/hbase/data/lib/UDFRectangleDemoLWD.jar'";//创建UDF函数的SQL语句，URL路径一定要配置好，否则会出错
        //RectangleQuery2:只有UDF函数的SQL语句字符串，UDF函数中矩形查询范围是否可以改造成字符串变量，可扩展性
        String sqlUDFConstraint = rQS.xLongitudeBL+","+rQS.yLatitudeBL+","+rQS.xLongitudeTR+","+rQS.yLatitudeTR;
        String sqlUDFQuery = "select * from geoPointTableLWD1MGS " +
                "where RectangleQueryUDFFunction(xLongitude,yLatitude,'"+sqlUDFConstraint+"')=1";//字符串拼接技巧
        try {
            //stmt.execute(sqlDropRectangleQueryUDFFunction);//删除UDF函数，该函数只执行一次
            //stmt.execute(sqlCreateRectangleQueryUDFFunction);//创建UDF函数，该函数只执行一次
            rs = stmt.executeQuery(sqlUDFQuery);
            //此处判断不能用rs.next()，否则最终得到的结果集会少一个
            if(rs.wasNull()){
                System.out.println("未找到记录，查询出错！");
                System.exit(-1);
            }
            while(rs.next()){
                int geoID = rs.getInt("geoID");
                String geoName = rs.getString("geoName");
                double xLongitude = rs.getDouble("xLongitude");
                double yLatitude = rs.getDouble("yLatitude");
                String geoHashValue = rs.getString("geoHashValue");
                long geoHashValueLong = rs.getLong("geoHashValueLong");
                GeoPointTableRecord g = new GeoPointTableRecord(geoID,geoName,xLongitude,yLatitude,geoHashValue,geoHashValueLong);
                geoPointTableRecords.add(g);
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }//执行SQL语句并保存结果
        return geoPointTableRecords;
    }
    /**
     * 函数功能：Query3.1.1 GoeHash+客户端二次过程，但此次的geoHashValue为字符串类型，Base32编码
     * @param rQS 矩形查询范围对象
     * @param searchDepthManual geoHash搜索深度，需要手动设置，以后可能会修改
     * @return 矩形查询后的记录
     */
    public static ArrayList<GeoPointTableRecord> selectAndQueryRecordsWithStringGeoHashIndexAndSecondFiltering(RectangleQueryScope rQS,int searchDepthManual){
        ResultSet rs;//查询结果集
        ArrayList<GeoPointTableRecord> geoPointTableRecords = new ArrayList<GeoPointTableRecord>();//保存查询结果记录的数组
        //GeoHash索引部分
        //ArrayList<long[]> geoHashLongQueryResults =
                //GeoHashConversion.rangeQueryWithGeoHashIndexAccordingToRectangleQueryScore(rQS,searchDepthManual);
        ArrayList<String[]> strBinaryGeoHashQueryResults =
                GeoHashConversion.rangeQueryWithGeoHashIndexAccordingToRectangleQueryScore60(rQS,searchDepthManual);
        //输出测试
//        for(String[] g:strBinaryGeoHashQueryResults){
//            System.out.println(GeoHashConversion.base32(Long.parseLong(g[0], 2))+"#"+GeoHashConversion.base32(Long.parseLong(g[1], 2)));
//        }
        String sqlWhereConstraint = "";//SQL语句
        int gLength = strBinaryGeoHashQueryResults.size();//获取geoHash段的个数
        //输出测试
        //System.out.println(gLength);//输出长度
        //如果geoHash段个数大于1
        if(gLength>1){
            for(int i=0;i<gLength-1;i++){
                String[] gMinMax = strBinaryGeoHashQueryResults.get(i);
                sqlWhereConstraint += " geoHashValue between '"
                        +GeoHashConversion.base32(Long.parseLong(gMinMax[0],2))+"' and '"+GeoHashConversion.base32(Long.parseLong(gMinMax[1], 2))+"' or ";
            }
        }
        //最后一个geoHash段
        String[] gMinMaxEnd = strBinaryGeoHashQueryResults.get(gLength-1);
        //geoHash段的SQL语句字符串
        sqlWhereConstraint +="geoHashValue between '"+
                GeoHashConversion.base32(Long.parseLong(gMinMaxEnd[0], 2))+"' and '"+GeoHashConversion.base32(Long.parseLong(gMinMaxEnd[1], 2))+"'";
        //RectangleQuery3:geoHash段的SQL语句字符串,把数据返回后在客户端进行二次过滤（李老师提出的心得测试方法）
//        String sqlGeoHashQuery = "Select /*+ index(geoPointTableLWD1MGS idx_geoHash_lwdgs)*/ " +
//                " * from geoPointTableLWD1MGS where "+sqlWhereConstraint;
        //String sqlGeoHashQuery = "Select geoID,geoName,xLongitude,yLatitude,GeoHashValue,geoHashValueLong from geoPointTableLWD1MGS where "+sqlWhereConstraint;
        String sqlGeoHashQuery = "Select xLongitude,yLatitude from GeoPointTable1MGeoHashLong60 where "+sqlWhereConstraint;
        try {
            rs = stmt.executeQuery(sqlGeoHashQuery);
            //此处判断不能用rs.next()，否则最终得到的结果集会少一个，但这个判空函数是错误的
            if(rs.wasNull()){
                System.out.println("未找到记录，查询出错！");
                System.exit(-1);
            }
            int count=0;
            while(rs.next()){
                count++;
                //int geoID = rs.getInt("geoID");
                //String geoName = rs.getString("geoName");
                double xLongitude = rs.getDouble("xLongitude");
                double yLatitude = rs.getDouble("yLatitude");
                //String geoHashValue = rs.getString("geoHashValue");
                //long geoHashValueLong = rs.getLong("geoHashValueLong");
                //GeoPointTableRecord g = new GeoPointTableRecord(geoID,geoName,xLongitude,yLatitude,geoHashValue,geoHashValueLong);
                GeoPointTableRecord g = new GeoPointTableRecord();
                g.xLongitude = xLongitude;
                g.yLatitude = yLatitude;
                if(rQS.isContainGeoPointTableRecord(g)){
                    geoPointTableRecords.add(g);
                }
            }
            System.out.println("-------------------"+count+"=======================");
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }//执行SQL语句并保存结果
        return geoPointTableRecords;
    }
    /**
     * 函数功能：Query3.1 GeoHash+客户端二次过滤，建立GeoHash索引，根据索引查出记录集，返回本地内存，然后进行二次过滤
     * 分析：本查询方式耗时的地方有两个，首先是GeoHash索引段SQL语句的解析，要想提高解析速度，需对GeoHash列建立二级索引，并采用优化的SQL方式，
     * 二是返回记录集的大小，如果GeoHash索引建的好，返回的记录集比较小，这样，本地内存的过滤效果肯定特别高
     * @param rQS 矩形查询范围对象
     * @param searchDepthManual geoHash搜索深度，需要手动设置，以后可能会修改
     * @return 矩形查询后的记录
     */
    public static ArrayList<GeoPointTableRecord> selectAndQueryRecordsWithGeoHashIndexAndSecondFilteringUnionAll(RectangleQueryScope rQS,int searchDepthManual){
        ResultSet rs;//查询结果集
        ArrayList<GeoPointTableRecord> geoPointTableRecords = new ArrayList<GeoPointTableRecord>();//保存查询结果记录的数组
        //GeoHash索引部分
        ArrayList<long[]> geoHashLongQueryResults =
            GeoHashConversion.rangeQueryWithGeoHashIndexAccordingToRectangleQueryScore(rQS,searchDepthManual);
        //输出测试
//        for(long[] g:geoHashLongQueryResults){
//            System.out.println(g[0]+"#"+g[1]);
//        }
        int gLength = geoHashLongQueryResults.size();//获取geoHash段的个数
        //System.out.println(gLength);//输出长度
        //如果geoHash段个数大于1
        String sqlWhereConstraintBetween = "";//SQL语句
        String sqlWhereConstraintDirectJudge = "";
        String sqlGeoHashQueryUnionAllBetweenAnd = "";
        if(gLength>1){
            for(int i=0;i<gLength-1;i++){
                long[] gMinMax = geoHashLongQueryResults.get(i);
                sqlWhereConstraintBetween += " geoHashValueLong between " +gMinMax[0]+" and "+gMinMax[1]+" or ";
                sqlWhereConstraintDirectJudge += "geoHashValueLong >= "+gMinMax[0]+" and geoHashValueLong <= "+gMinMax[1]+" or ";
                sqlGeoHashQueryUnionAllBetweenAnd += "Select geoID,geoName,xLongitude,yLatitude,geoHashValue,geoHashValueLong " +
                        "from geoPointTableLWD1MGS where geoHashValueLong between "+ gMinMax[0]+" and "+gMinMax[1]+" UNION ALL ";
            }
        }
        //最后一个geoHash段
        long[] gMinMaxEnd = geoHashLongQueryResults.get(gLength-1);
        //geoHash段的SQL语句字符串
        sqlWhereConstraintBetween +="geoHashValueLong between "+ gMinMaxEnd[0]+" and "+gMinMaxEnd[1];
        sqlWhereConstraintDirectJudge += "geoHashValueLong >= "+gMinMaxEnd[0]+" and geoHashValueLong <= "+gMinMaxEnd[1];
        //3.1.4 Union All的使用
        sqlGeoHashQueryUnionAllBetweenAnd += "Select geoID,geoName,xLongitude,yLatitude,geoHashValue,geoHashValueLong " +
                "from geoPointTableLWD1MGS where geoHashValueLong between "+ gMinMaxEnd[0]+" and "+gMinMaxEnd[1];
        //RectangleQuery3:geoHash段的SQL语句字符串,把数据返回后在客户端进行二次过滤（李老师提出的心得测试方法）
//        String sqlGeoHashQuery = "Select /*+ index(geoPointTableLWD1MGS idx_geoHash_lwdgs)*/ " +
//                " * from geoPointTableLWD1MGS where "+sqlWhereConstraint;
        //String sqlGeoHashQuery = "Select geoID,geoName,xLongitude,yLatitude,GeoHashValue,geoHashValueLong from geoPointTableLWD1MGS where "+sqlWhereConstraint;
        //Query3.1.1 or起来的between and
        String sqlGeoHashQueryBetweenAnd = "Select geoID,geoName,xLongitude,yLatitude,geoHashValue,geoHashValueLong " +
                "from geoPointTableLWD1MGS where "+sqlWhereConstraintBetween;
        //Query3.1.2 or起来的直接查询，与Query3.1.1效率差不多，但Query3.1.1和Query3.1.2在建立Phoenix二级索引后，效率反而降低，并且降低的很多
        String sqlGeoHashQueryDirectJudge = "Select geoID,geoName,xLongitude,yLatitude,geoHashValue,geoHashValueLong " +
                "from geoPointTableLWD1MGS where "+sqlWhereConstraintDirectJudge;
        //Query3.1.3 不使用or起来的between and 方式，直接对每个or进行查询，然后将结果汇总起来，实验表明，这种方式比Query3.1.1和Query3.1.2效率更慢
        //但是，建立Phoenix二级索引后，奇迹出现了，查询效率获得明显提升。
//        int count = 0;
//        for(long[] gMinMaxNoOr: geoHashLongQueryResults){
//            String sqlGeoHashQueryNoOrBetweenAnd = "Select geoID,geoName,xLongitude,yLatitude,geoHashValue,geoHashValueLong " +
//                    "from geoPointTableLWD1MGS where geoHashValueLong between "+ gMinMaxNoOr[0]+" and "+gMinMaxNoOr[1];
//            try {
//                rs = stmt.executeQuery(sqlGeoHashQueryNoOrBetweenAnd);
//                //此处判断不能用rs.next()，否则最终得到的结果集会少一个，但这个判空函数是错误的
//                if(rs.wasNull()){
//                    System.out.println("未找到记录，查询出错！");
//                    System.exit(-1);
//                }
//                //int count=0;
//                while(rs.next()){
//                    count++;
//                    int geoID = rs.getInt("geoID");
//                    String geoName = rs.getString("geoName");
//                    double xLongitude = rs.getDouble("xLongitude");
//                    double yLatitude = rs.getDouble("yLatitude");
//                    String geoHashValue = rs.getString("geoHashValue");
//                    long geoHashValueLong = rs.getLong("geoHashValueLong");
//                    GeoPointTableRecord g = new GeoPointTableRecord(geoID,geoName,xLongitude,yLatitude,geoHashValue,geoHashValueLong);
////                GeoPointTableRecord g = new GeoPointTableRecord();
////                g.xLongitude = xLongitude;
////                g.yLatitude = yLatitude;
//                    if(rQS.isContainGeoPointTableRecord(g)){
//                        geoPointTableRecords.add(g);
//                    }
//                }
//                //System.out.println("-------------------"+count+"=======================");
//            } catch (SQLException e) {
//                // TODO Auto-generated catch block
//                e.printStackTrace();
//            }//执行SQL语句并保存
//
//        }
//        System.out.println("-------------------"+count+"=======================");
        //


        try {
            rs = stmt.executeQuery(sqlGeoHashQueryUnionAllBetweenAnd);
            //此处判断不能用rs.next()，否则最终得到的结果集会少一个，但这个判空函数是错误的
            if(rs.wasNull()){
                System.out.println("未找到记录，查询出错！");
                System.exit(-1);
            }
            int count=0;
            while(rs.next()){
                count++;
                int geoID = rs.getInt("geoID");
                String geoName = rs.getString("geoName");
                double xLongitude = rs.getDouble("xLongitude");
                double yLatitude = rs.getDouble("yLatitude");
                String geoHashValue = rs.getString("geoHashValue");
                long geoHashValueLong = rs.getLong("geoHashValueLong");
                GeoPointTableRecord g = new GeoPointTableRecord(geoID,geoName,xLongitude,yLatitude,geoHashValue,geoHashValueLong);
//                GeoPointTableRecord g = new GeoPointTableRecord();
//                g.xLongitude = xLongitude;
//                g.yLatitude = yLatitude;
                if(rQS.isContainGeoPointTableRecord(g)){
                    geoPointTableRecords.add(g);
                }
            }
            System.out.println("-------------------"+count+"=======================");
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }//执行SQL语句并保存结果
        return geoPointTableRecords;
    }
    /**
     * 函数功能：Query3.2 GeoHash+直接判断
     * @param rQS 矩形查询范围对象
     * @param searchDepthManual geoHash搜索深度，需要手动设置，以后可能会修改
     * @return 矩形查询后的记录
     */
    public static ArrayList<GeoPointTableRecord> selectAndQueryRecordsWithGeoHashIndexAndDirectJudge(RectangleQueryScope rQS,int searchDepthManual){
        ResultSet rs;//查询结果集
        ArrayList<GeoPointTableRecord> geoPointTableRecords = new ArrayList<GeoPointTableRecord>();//保存查询结果记录的数组
        //GeoHash算法，递归查询，获得多个GeoHash段

        //矩形查询范围的左下角点和右上角点的坐标
        double xLongitudeBLQS = rQS.xLongitudeBL;
        double yLatitudeBLQS = rQS.yLatitudeBL;
        double xLongitudeTRQS = rQS.xLongitudeTR;
        double yLatitudeTRQS = rQS.yLatitudeTR;
        //GeoHash索引部分
        ArrayList<long[]> geoHashLongQueryResults =
                GeoHashConversion.rangeQueryWithGeoHashIndexAccordingToRectangleQueryScore(rQS,searchDepthManual);
        //输出测试
        //for(long[] g:geoHashLongQueryResults){
        //System.out.println(g[0]+"#"+g[1]);
        //}
        String sqlWhereConstraint = "";//SQL语句
        int gLength = geoHashLongQueryResults.size();//获取geoHash段的个数
        //输出测试
        //System.out.println(gLength);//输出长度
        //如果geoHash段个数大于1
        if(gLength>1){
            for(int i=0;i<gLength-1;i++){
                long[] gMinMax = geoHashLongQueryResults.get(i);
                sqlWhereConstraint += " geoHashValueLong between "+gMinMax[0]+" and "+gMinMax[1]+" or ";
            }
        }
        //最后一个geoHash段
        long[] gMinMaxEnd = geoHashLongQueryResults.get(gLength-1);
        //geoHash段的SQL语句字符串
        sqlWhereConstraint +="geoHashValueLong between "+gMinMaxEnd[0]+" and "+gMinMaxEnd[1];
        //RectangleQuery4:geoHash段+直接判断的SQL语句字符串
        String sqlGeoHashDirectJudgeQuery = "Select * from geoPointTableLWD1MGP where ("+sqlWhereConstraint+" ) and "
                + "xLongitude>="+xLongitudeBLQS+" and "+"xLongitude <= "+xLongitudeTRQS+ " and "
                +"yLatitude >= "+yLatitudeBLQS+" and "+"yLatitude <= "+yLatitudeTRQS;
        try {
            rs = stmt.executeQuery(sqlGeoHashDirectJudgeQuery);
            //此处判断不能用rs.next()，否则最终得到的结果集会少一个
            if(rs.wasNull()){
                System.out.println("未找到记录，查询出错！");
                System.exit(-1);
            }
            while(rs.next()){
                int geoID = rs.getInt("geoID");
                String geoName = rs.getString("geoName");
                double xLongitude = rs.getDouble("xLongitude");
                double yLatitude = rs.getDouble("yLatitude");
                String geoHashValue = rs.getString("geoHashValue");
                long geoHashValueLong = rs.getLong("geoHashValueLong");
                GeoPointTableRecord g = new GeoPointTableRecord(geoID,geoName,xLongitude,yLatitude,geoHashValue,geoHashValueLong);
                geoPointTableRecords.add(g);
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }//执行SQL语句并保存结果
        return geoPointTableRecords;
    }
    /**
     * 函数功能：Query3.2 GeoHash(UnionAll)+直接判断
     * @param rQS 矩形查询范围对象
     * @param searchDepthManual geoHash搜索深度，需要手动设置，以后可能会修改
     * @return 矩形查询后的记录
     */
    public static ArrayList<GeoPointTableRecord> selectAndQueryRecordsWithGeoHashIndexAndDirectJudgeUnionAll(RectangleQueryScope rQS,int searchDepthManual){
        ResultSet rs;//查询结果集
        ArrayList<GeoPointTableRecord> geoPointTableRecords = new ArrayList<GeoPointTableRecord>();//保存查询结果记录的数组
        //GeoHash索引部分
        ArrayList<long[]> geoHashLongQueryResults =
                GeoHashConversion.rangeQueryWithGeoHashIndexAccordingToRectangleQueryScore(rQS,searchDepthManual);
        String strDirectJudge = "xLongitude>="+rQS.xLongitudeBL+" and "+"xLongitude <= "+rQS.xLongitudeTR
                + " and "+"yLatitude >= "+rQS.yLatitudeBL+" and "+"yLatitude <= "+rQS.yLatitudeTR;
//        String sqlDirectJudgeQuery = "select geoID,geoName,xLongitude,yLatitude,geoHashValue,geoHashValueLong from geoPointTableLWD1MGS where " +
//                "xLongitude>="+rQS.xLongitudeBL+" and "+"xLongitude <= "+rQS.xLongitudeTR
//                + " and "+"yLatitude >= "+rQS.yLatitudeBL+" and "+"yLatitude <= "+rQS.yLatitudeTR;
        //输出测试
        //for(long[] g:geoHashLongQueryResults){
        //System.out.println(g[0]+"#"+g[1]);
        //}
        //String sqlGeoHashQueryUnionAllBetweenAnd = sqlDirectJudgeQuery+" Union all  ";
        String sqlGeoHashQueryUnionAllBetweenAnd = "";
        int gLength = geoHashLongQueryResults.size();//获取geoHash段的个数
        //输出测试
        //System.out.println(gLength);//输出长度.
        //如果geoHash段个数大于1
        if(gLength>1){
            for(int i=0;i<gLength-1;i++){
                long[] gMinMax = geoHashLongQueryResults.get(i);
                //sqlWhereConstraint += " geoHashValueLong between "+gMinMax[0]+" and "+gMinMax[1]+" or ";
                sqlGeoHashQueryUnionAllBetweenAnd += "Select geoID,geoName,xLongitude,yLatitude,geoHashValue,geoHashValueLong " +
                        "from geoPointTableLWD1MGS where geoHashValueLong between "+ gMinMax[0]+" and "+gMinMax[1]+" and "+strDirectJudge+" UNION ALL ";
            }
        }
        //最后一个geoHash段
        long[] gMinMaxEnd = geoHashLongQueryResults.get(gLength-1);
        //geoHash段的SQL语句字符串
        //sqlWhereConstraint +="geoHashValueLong between "+gMinMaxEnd[0]+" and "+gMinMaxEnd[1];

        sqlGeoHashQueryUnionAllBetweenAnd += "Select geoID,geoName,xLongitude,yLatitude,geoHashValue,geoHashValueLong " +
                "from geoPointTableLWD1MGS where geoHashValueLong between "+ gMinMaxEnd[0]+" and "+gMinMaxEnd[1]+" and "+strDirectJudge;
        //RectangleQuery4:geoHash段+直接判断的SQL语句字符串
//        String sqlGeoHashDirectJudgeQuery = "Select * from geoPointTableLWD1MGP where ("+sqlWhereConstraint+" ) and "
//                + "xLongitude>="+xLongitudeBLQS+" and "+"xLongitude <= "+xLongitudeTRQS+ " and "
//                +"yLatitude >= "+yLatitudeBLQS+" and "+"yLatitude <= "+yLatitudeTRQS;
        try {
            rs = stmt.executeQuery(sqlGeoHashQueryUnionAllBetweenAnd);
            //rs = stmt.executeQuery(sqlDirectJudgeQuery);
            //此处判断不能用rs.next()，否则最终得到的结果集会少一个
            if(rs.wasNull()){
                System.out.println("未找到记录，查询出错！");
                System.exit(-1);
            }
            while(rs.next()){
                int geoID = rs.getInt("geoID");
                String geoName = rs.getString("geoName");
                double xLongitude = rs.getDouble("xLongitude");
                double yLatitude = rs.getDouble("yLatitude");
                String geoHashValue = rs.getString("geoHashValue");
                long geoHashValueLong = rs.getLong("geoHashValueLong");
                GeoPointTableRecord g = new GeoPointTableRecord(geoID,geoName,xLongitude,yLatitude,geoHashValue,geoHashValueLong);
                geoPointTableRecords.add(g);
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }//执行SQL语句并保存结果
        return geoPointTableRecords;
    }
    /**
     * 函数功能：Query3.3 GeoHash+UDF函数
     * @param rQS 矩形查询范围对象
     * @param searchDepthManual geoHash搜索深度，需要手动设置，以后可能会修改
     * @return 矩形查询后的记录
     */
    public static ArrayList<GeoPointTableRecord> selectAndQueryRecordsWithGeoHashIndexAndUDFFunction(RectangleQueryScope rQS,int searchDepthManual){
        ResultSet rs;//查询结果集
        ArrayList<GeoPointTableRecord> geoPointTableRecords = new ArrayList<GeoPointTableRecord>();//保存查询结果记录的数组
        //UDF函数操作
        String sqlDropRectangleQueryUDFFunction = "DROP FUNCTION IF EXISTS RectangleQueryUDFFunction";//删除UDF函数的SQL语句
        String sqlCreateRectangleQueryUDFFunction = "CREATE FUNCTION RectangleQueryUDFFunction(Double, Double, Varchar) " +
                "returns Integer as 'cc.xidian.PhoenixOperation.UDFRectangleDemoLWD' " +
                "using jar 'hdfs://cloudgis/apps/hbase/data/lib/UDFRectangleDemoLWD.jar'";//创建UDF函数的SQL语句，URL路径一定要配置好，否则会出错
        //GeoHash算法，递归查询，获得多个GeoHash段

        //矩形查询范围的左下角点和右上角点的坐标
        double xLongitudeBLQS = rQS.xLongitudeBL;
        double yLatitudeBLQS = rQS.yLatitudeBL;
        double xLongitudeTRQS = rQS.xLongitudeTR;
        double yLatitudeTRQS = rQS.yLatitudeTR;
        ArrayList<long[]> geoHashLongQueryResults =
                GeoHashConversion.rangeQueryWithGeoHashIndexAccordingToRectangleQueryScore(rQS,searchDepthManual);
        //输出测试
        //for(long[] g:geoHashLongQueryResults){
        //System.out.println(g[0]+"#"+g[1]);
        //}
        String sqlWhereConstraint = "";//SQL语句
        int gLength = geoHashLongQueryResults.size();//获取geoHash段的个数
        //输出测试
        //System.out.println(gLength);//输出长度
        //如果geoHash段个数大于1
        if(gLength>1){
            for(int i=0;i<gLength-1;i++){
                long[] gMinMax = geoHashLongQueryResults.get(i);
                sqlWhereConstraint += " geoHashValueLong between "+gMinMax[0]+" and "+gMinMax[1]+" or ";
            }
        }
        //最后一个geoHash段
        long[] gMinMaxEnd = geoHashLongQueryResults.get(gLength-1);
        //geoHash段的SQL语句字符串
        sqlWhereConstraint +="geoHashValueLong between "+gMinMaxEnd[0]+" and "+gMinMaxEnd[1];
        //RectangleQuery5:geoHash段+UDF函数的SQL语句字符串
        String sqlUDFConstraint = rQS.xLongitudeBL+","+rQS.yLatitudeBL+","+rQS.xLongitudeTR+","+rQS.yLatitudeTR;
        String sqlGeoHashUDFQuery = "Select * from geoPointTableLWD1MGP " +
                "where ("+sqlWhereConstraint+" ) and RectangleQueryUDFFunction(xLongitude,yLatitude,'"+sqlUDFConstraint+"')=1";
        try {
            //Statement stmtUDF = conn.createStatement();
            //stmtUDF.execute(sqlDropRectangleQueryUDFFunction);
//            stmtUDF.execute(sqlCreateRectangleQueryUDFFunction);
            rs = stmt.executeQuery(sqlGeoHashUDFQuery);
            //此处判断不能用rs.next()，否则最终得到的结果集会少一个
            if(rs.wasNull()){
                System.out.println("未找到记录，查询出错！");
                System.exit(-1);
            }
            while(rs.next()){
                int geoID = rs.getInt("geoID");
                String geoName = rs.getString("geoName");
                double xLongitude = rs.getDouble("xLongitude");
                double yLatitude = rs.getDouble("yLatitude");
                String geoHashValue = rs.getString("geoHashValue");
                long geoHashValueLong = rs.getLong("geoHashValueLong");
                GeoPointTableRecord g = new GeoPointTableRecord(geoID,geoName,xLongitude,yLatitude,geoHashValue,geoHashValueLong);
                geoPointTableRecords.add(g);
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }//执行SQL语句并保存结果
        return geoPointTableRecords;
    }
    /**
     * 函数功能：Query3.3 GeoHash+UDF函数
     * @param rQS 矩形查询范围对象
     * @param searchDepthManual geoHash搜索深度，需要手动设置，以后可能会修改
     * @return 矩形查询后的记录
     */
    public static ArrayList<GeoPointTableRecord> selectAndQueryRecordsWithGeoHashIndexAndUDFFunctionUnionAll(RectangleQueryScope rQS,int searchDepthManual){
        ResultSet rs;//查询结果集
        ArrayList<GeoPointTableRecord> geoPointTableRecords = new ArrayList<GeoPointTableRecord>();//保存查询结果记录的数组
        //UDF函数操作
        String sqlDropRectangleQueryUDFFunction = "DROP FUNCTION IF EXISTS RectangleQueryUDFFunction";//删除UDF函数的SQL语句
        String sqlCreateRectangleQueryUDFFunction = "CREATE FUNCTION RectangleQueryUDFFunction(Double, Double, Varchar) " +
                "returns Integer as 'cc.xidian.PhoenixOperation.UDFRectangleDemoLWD' " +
                "using jar 'hdfs://cloudgis/apps/hbase/data/lib/UDFRectangleDemoLWD.jar'";//创建UDF函数的SQL语句，URL路径一定要配置好，否则会出错
        String sqlUDFConstraint = rQS.xLongitudeBL+","+rQS.yLatitudeBL+","+rQS.xLongitudeTR+","+rQS.yLatitudeTR;
        String strUDFFunction = "RectangleQueryUDFFunction(xLongitude,yLatitude,'"+sqlUDFConstraint+"')=1";

        ArrayList<long[]> geoHashLongQueryResults =
                GeoHashConversion.rangeQueryWithGeoHashIndexAccordingToRectangleQueryScore(rQS,searchDepthManual);
        //输出测试
        //for(long[] g:geoHashLongQueryResults){
        //System.out.println(g[0]+"#"+g[1]);
        //}
        //String sqlWhereConstraint = "";//SQL语句
        String sqlGeoHashQueryUnionAllBetweenAndAndUDFFunction = "";
        int gLength = geoHashLongQueryResults.size();//获取geoHash段的个数
        //输出测试
        //System.out.println(gLength);//输出长度
        //如果geoHash段个数大于1
        if(gLength>1){
            for(int i=0;i<gLength-1;i++){
                long[] gMinMax = geoHashLongQueryResults.get(i);
                sqlGeoHashQueryUnionAllBetweenAndAndUDFFunction += "Select geoID,geoName,xLongitude,yLatitude,geoHashValue,geoHashValueLong " +
                        "from geoPointTableLWD1MGS where geoHashValueLong between "+ gMinMax[0]+" and "+gMinMax[1]
                        +" and "+strUDFFunction+" UNION ALL ";
            }
        }
        //最后一个geoHash段
        long[] gMinMaxEnd = geoHashLongQueryResults.get(gLength-1);
        sqlGeoHashQueryUnionAllBetweenAndAndUDFFunction += "Select geoID,geoName,xLongitude,yLatitude,geoHashValue,geoHashValueLong " +
                "from geoPointTableLWD1MGS where geoHashValueLong between "+ gMinMaxEnd[0]+" and "+gMinMaxEnd[1]+" and "+strUDFFunction;
        try {
            //Statement stmtUDF = conn.createStatement();
            //stmtUDF.execute(sqlDropRectangleQueryUDFFunction);
//            stmtUDF.execute(sqlCreateRectangleQueryUDFFunction);
            rs = stmt.executeQuery(sqlGeoHashQueryUnionAllBetweenAndAndUDFFunction);
            //此处判断不能用rs.next()，否则最终得到的结果集会少一个
            if(rs.wasNull()){
                System.out.println("未找到记录，查询出错！");
                System.exit(-1);
            }
            while(rs.next()){
                int geoID = rs.getInt("geoID");
                String geoName = rs.getString("geoName");
                double xLongitude = rs.getDouble("xLongitude");
                double yLatitude = rs.getDouble("yLatitude");
                String geoHashValue = rs.getString("geoHashValue");
                long geoHashValueLong = rs.getLong("geoHashValueLong");
                GeoPointTableRecord g = new GeoPointTableRecord(geoID,geoName,xLongitude,yLatitude,geoHashValue,geoHashValueLong);
                geoPointTableRecords.add(g);
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }//执行SQL语句并保存结果
        return geoPointTableRecords;
    }
    /**
     * 函数功能：构建二级索引，该函数只执行一次
     */
    public static void createSecondIndexForGeoPointTableLWD1MLong(){
        //对GeoHashValue列构建二级索引的SQL语句，不区分大小写，SQL语句
//        String sqlCreateIndex = "Create index idx_geoHash_lwdgs on GeoPointTableLWD1MGS (GeoHashValueLong) " +
//                "include (GeoID,geoName,xLongitude,yLatitude,GeoHashValue)";
        String sqlCreateIndex = "Create index idx_geoHash_lwdgs on GeoPointTableLWD1MGS (GeoHashValueLong DESC) " +
                "include (xLongitude,yLatitude)";
        try {
            stmt.executeUpdate(sqlCreateIndex);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    /**
     * 函数功能：构建二级索引，该函数只执行一次
     */
    public static void createSecondIndexForGeoPointTableLWD1MLongXY(){
        //对GeoHashValue列构建二级索引的SQL语句，不区分大小写，SQL语句
        String sqlCreateIndex = "Create index idx_geoHash_lwd_Cover on GeoPointTableLWD1MLong(GeoHashValueLong) " +
                "include(geoID,geoName,xLongitude,yLatitude,geoHashValue)";
        try {
            stmt.executeUpdate(sqlCreateIndex);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    /**
     * 函数功能：对字符串类型的geoName列建二级索引，该函数只执行一次，注意表名和索引列名的一致性
     * 编写：刘文东
     * 时间：2016年9月21日23:09:01
     */
    public static void createSecondIndexForGeoNameOfTable(){
        //对GeoHashValue列构建二级索引的SQL语句，不区分大小写，SQL语句
        String sqlCreateIndex = "Create index idx_geoName_lwd on GeoPointTableLWD1MGS(geoName) " +
                "include(yLatitude,geoHashValue)";
        try {
            stmt.executeUpdate(sqlCreateIndex);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    /**
     * 函数功能：对BigInt类型的GeoHashValueLong建立二级索引，注意表名和索引列的一致，该函数只执行一次
     * 编码：刘文东
     * 时间：2016年9月22日23:09:23
     */
    public static void createSecondIndexForGeoHashValueLongOfTable(){
        //对GeoHashValue列构建二级索引的SQL语句，不区分大小写，SQL语句
        String sqlCreateIndex = "Create index idx_geoHashValueLong_lwd on GeoPointTableLWD1MGS(geoHashValueLong) " +
                "include(geoID,geoName,xLongitude,yLatitude,geoHashValue)";
        try {
            stmt.executeUpdate(sqlCreateIndex);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * 函数功能：对字符串类型Base32编码的GeoHashValue创建二级索引，注意表名和索引列的名称一致，该函数只执行一次
     * 编码：刘文东
     * 时间：2016年9月22日23:04:00
     */
    public static void createSecondIndexForGeoHashValueBase32OfTable(){
        //对GeoHashValue列构建二级索引的SQL语句，不区分大小写，SQL语句
        String sqlCreateIndex = "Create index idx_geoHashValue_lwd on GeoPointTable1MGeoHashLong60(geoHashValue) " +
                "include(geoID,geoName,xLongitude,yLatitude,geoHashValueLong)";
        try {
            stmt.executeUpdate(sqlCreateIndex);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    /**
     * 函数功能：删除二级索引，该函数只执行一次，修改时，要注意
     */
    public static void dropIndexOfName(){
        String sqlDropIndexOfName = "Drop index idx_geoHashValueLong_lwd on GeoPointTableLWD1MGS";
        try {
            stmt.executeUpdate(sqlDropIndexOfName);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    /**
     * 函数功能：根据随机生成的geoID获得字符串类型的geoNames集合，判断Phoenix二级索引对varchar类型是否起作用
     * 编码：刘文东
     * 时间：2016年9月21日22:53:31
     * @return 字符串类型的geoNames集合
     */
    public static ArrayList<String> getGeoNamesSelectByRandomGeoIDToTestPhoenixSecondIndex(){
        ResultSet rs;
        ArrayList<String> geoNamesSelectByGeoID = new ArrayList<String>();
        try {
        for(int i=0;i<100;i++) {
            int geoIDRandom = Integer.parseInt(RandomString.getNum(5));
            String sqlSelectByRandomGeoID = "select geoName from GeoPointTableLWD1MGS where geoID = "+geoIDRandom;
            rs = stmt.executeQuery(sqlSelectByRandomGeoID);
            //此处判断不能用rs.next()，否则最终得到的结果集会少一个
            if (rs.wasNull()) {
                System.out.println("未找到记录，查询出错！");
                System.exit(-1);
            }
            while (rs.next()) {
                String geoName = rs.getString("geoName");
                geoNamesSelectByGeoID.add(geoName);
            }
        }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }//执行SQL语句并保存结果
        return geoNamesSelectByGeoID;
    }

    /**
     * 函数功能：根据字符串类型的geoNames集合进行查询，判断Phoenix二级索引对varchar类型是否起作用
     * 编码：刘文东
     * 时间：2016年9月21日22:54:41
     * @param geoNames 字符串类型的geoNames集合
     */
    public static void selectAndQueryByGeoNamesToTestPhoenixSecondIndex(ArrayList<String> geoNames){
        ResultSet rs;
        try {
            for(String s:geoNames) {
                //int geoIDRandom = Integer.parseInt(RandomString.getNum(5));
                String sqlSelectByGeoName = "select * from GeoPointTableLWD1MGS where geoName = '"+s+"'";
                rs = stmt.executeQuery(sqlSelectByGeoName);
                //此处判断不能用rs.next()，否则最终得到的结果集会少一个
                if (rs.wasNull()) {
                    System.out.println("未找到记录，查询出错！");
                    System.exit(-1);
                }
                while (rs.next()) {
                    int geoID = rs.getInt("geoID");
                    String geoName = rs.getString("geoName");
                    //geoNamesSelectByGeoID.add(geoName);
                    double xLongitude = rs.getDouble("xLongitude");
                    double yLatitude = rs.getDouble("yLatitude");
                    String geoHashValue = rs.getString("geoHashValue");
                    long geoHashValueLong = rs.getLong("geoHashValueLong");
                    GeoPointTableRecord g = new GeoPointTableRecord(geoID, geoName, xLongitude, yLatitude, geoHashValue, geoHashValueLong);
                }
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }//执行SQL语句并保存结果
    }

    /**
     * 函数功能：根据随机生成的geoID得到BigInt类型的geoHashValueLongs，用于后续判断Phoenix二级索引对BitInt列是否起作用
     * 编码：刘文东
     * 时间：2016年9月22日22:51:25
     * @return  BigInt类型的geoHashValueLongs集合
     */
    public static long[] getGeoHashValueLongsSelectByRandomGeoIDToTestPhoenixSecondIndex(){
        ResultSet rs;
        long[] geoHashValueLongsByGeoID = new long[105];
        try {
            for(int i=0;i<100;i++) {
                int geoIDRandom = Integer.parseInt(RandomString.getNum(5));
                String sqlSelectByRandomGeoID = "select geoHashValueLong from GeoPointTableLWD1MGS where geoID = "+geoIDRandom;
                rs = stmt.executeQuery(sqlSelectByRandomGeoID);
                //此处判断不能用rs.next()，否则最终得到的结果集会少一个
                if (rs.wasNull()) {
                    System.out.println("未找到记录，查询出错！");
                    System.exit(-1);
                }
                while (rs.next()) {
                    long geoHashValueLong = rs.getLong("geoHashValueLong");
                    geoHashValueLongsByGeoID[i] = geoHashValueLong;
                }
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }//执行SQL语句并保存结果
        return geoHashValueLongsByGeoID;
    }

    /**
     * 函数功能：根据BigInt类型的geoHashValueLongs集合进行查询，判断Phoenix二级索引对BitInt列是否起作用
     * 编码：刘文东
     * 时间：2016年9月22日22:50:30
     * @param geoHashValues BigInt类型的geoHashValueLongs集合
     */
    public static void selectAndQueryByGeoHashValueLongsToTestPhoenixSecondIndex(long[] geoHashValues){
        ResultSet rs;
        try {
            for(long s:geoHashValues) {
                //int geoIDRandom = Integer.parseInt(RandomString.getNum(5));
                String sqlSelectByGeoHashValueLongs = "select * from GeoPointTableLWD1MGS where geoHashValueLong = "+s;
                rs = stmt.executeQuery(sqlSelectByGeoHashValueLongs);
                //此处判断不能用rs.next()，否则最终得到的结果集会少一个
                if (rs.wasNull()) {
                    System.out.println("未找到记录，查询出错！");
                    System.exit(-1);
                }
                while (rs.next()) {
                    int geoID = rs.getInt("geoID");
                    String geoName = rs.getString("geoName");
                    //geoNamesSelectByGeoID.add(geoName);
                    double xLongitude = rs.getDouble("xLongitude");
                    double yLatitude = rs.getDouble("yLatitude");
                    String geoHashValue = rs.getString("geoHashValue");
                    long geoHashValueLong = rs.getLong("geoHashValueLong");
                    GeoPointTableRecord g = new GeoPointTableRecord(geoID, geoName, xLongitude, yLatitude, geoHashValue, geoHashValueLong);
                }
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }//执行SQL语句并保存结果
    }
    /**
     * 函数功能：根据随机ID查找出一个GeoHashValue集合，用以判断Phoenix对表的geoHashValue列所建的二级索引是否正确
     * 编码：刘文东
     * 时间：2016年9月22日21:36:44
     * @return GeoHashValue集合
     */
    public static ArrayList<String> getGeoHashValuesSelectByRandomGeoIDToTestPhoenixSecondIndex(){
        ResultSet rs;
        ArrayList<String> geoHashValuesSelectByGeoID = new ArrayList<String>();
        try {
            for(int i=0;i<100;i++) {
                int geoIDRandom = Integer.parseInt(RandomString.getNum(5));
                String sqlSelectByRandomGeoID = "select geoHashValue from GeoPointTable1MGeoHashLong60 where geoID = "+geoIDRandom;
                rs = stmt.executeQuery(sqlSelectByRandomGeoID);
                //此处判断不能用rs.next()，否则最终得到的结果集会少一个
                if (rs.wasNull()) {
                    System.out.println("未找到记录，查询出错！");
                    System.exit(-1);
                }
                while (rs.next()) {
                    String geoHashValue = rs.getString("geoHashValue");
                    geoHashValuesSelectByGeoID.add(geoHashValue);
                }
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }//执行SQL语句并保存结果
        return geoHashValuesSelectByGeoID;
    }

    /**
     * 函数功能：根据随机ID查询的geoHashValue集合进行查询，观察Phoenix二级索引是否有效
     * 编码：刘文东
     * 时间：2016年9月22日21:37:17
     * @param geoHashValues geoHashValue集合
     */
    public static void selectAndQueryByGeoHashValuesToTestPhoenixSecondIndex(ArrayList<String> geoHashValues){
        ResultSet rs;
        try {
            for(String s:geoHashValues) {
                //int geoIDRandom = Integer.parseInt(RandomString.getNum(5));
                String sqlSelectByGeoHashValue = "select * from GeoPointTable1MGeoHashLong60 where geoHashValue = '"+s+"'";
                rs = stmt.executeQuery(sqlSelectByGeoHashValue);
                //此处判断不能用rs.next()，否则最终得到的结果集会少一个
                if (rs.wasNull()) {
                    System.out.println("未找到记录，查询出错！");
                    System.exit(-1);
                }
                while (rs.next()) {
                    int geoID = rs.getInt("geoID");
                    String geoName = rs.getString("geoName");
                    //geoNamesSelectByGeoID.add(geoName);
                    double xLongitude = rs.getDouble("xLongitude");
                    double yLatitude = rs.getDouble("yLatitude");
                    String geoHashValue = rs.getString("geoHashValue");
                    long geoHashValueLong = rs.getLong("geoHashValueLong");
                    GeoPointTableRecord g = new GeoPointTableRecord(geoID, geoName, xLongitude, yLatitude, geoHashValue, geoHashValueLong);
                }
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }//执行SQL语句并保存结果
    }
}
