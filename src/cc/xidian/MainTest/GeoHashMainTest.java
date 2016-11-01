package cc.xidian.MainTest;

import cc.xidian.GeoHash.GeoHashConversion;
import cc.xidian.GeoHash.RectanglePrefix;
import cc.xidian.GeoHash.RectangleStrBinaryPrefix;
import cc.xidian.GeoObject.GeoHashIndexRecord;
import cc.xidian.GeoObject.GeoPointTableRecord;
import cc.xidian.GeoObject.GeoPointTableRecordSimple;
import cc.xidian.GeoObject.RectangleQueryScope;
import cc.xidian.PhoenixOperation.PhoenixSQLOperation;
import cc.xidian.geoUtil.FileUtil;
import org.w3c.dom.css.Rect;

import java.io.File;
import java.lang.reflect.Array;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

/**
 * Created by hadoop on 2016/9/8.
 */
public class GeoHashMainTest {
    public static void main(String[] args) throws Exception{
        PhoenixSQLOperation.getConnectionWithHBase();//建立连接

        //1、通过Phoenix在HBase上的操作
        //1.2 删除表操作，该操作只执行一次
//        PhoenixSQLOperation.dropTableNamedGeoPointTable();//只执行一次
        //1.1 创建表并插入数据操作，该操作只执行一次
//        long startTimeCreateAndInsertRecords = System.currentTimeMillis();
        //PhoenixSQLOperation.createAndInsertRecordToTableNamedGeoPointTable100M();//只执行一次
        //PhoenixSQLOperation.createAndInsertRecordToTableNamedGeoPointTable1MWithLocalIndex();//建表后先创建Phoenix局部索引，然后再插入数据
//        PhoenixSQLOperation.createAndInsertRecordToTableNamedGeoPointTable10M();//创建一个10M记录的表，分区数为6，建立局部索引，并创建数据
//        long endTimeCreateAndInsertRecords = System.currentTimeMillis();
//        System.out.println("CreateAndInsertRecords-Time: "+(endTimeCreateAndInsertRecords - startTimeCreateAndInsertRecords));

        //以追加的方式写入记录
//        long startTimeInsert1MRecords = System.currentTimeMillis();
//        PhoenixSQLOperation.insertRecordToTableNamedGeoPointTable();
//        long endTimeInsert1MRecords = System.currentTimeMillis();
//        System.out.println("CreateAndInsertRecords-Time: "+(endTimeInsert1MRecords - startTimeInsert1MRecords));

        //1.3 查询并写入文件操作，该操作只执行一次，目的是获取表中部分数据并进行查看
//        long startTimeWriteToFile = System.currentTimeMillis();
//        PhoenixSQLOperation.selectResultsToFile();//只执行一次
//        long endTimeWriteToFile = System.currentTimeMillis();
//        System.out.println("WriteToFile-Time: "+(endTimeWriteToFile-startTimeWriteToFile));
        //1.4 删除Phoenix二级索引操作，该操作只执行一次
//        PhoenixSQLOperation.dropIndexOfName();
////////        //1.5 创建二级索引操作，并计时，该操作只执行一次
//        long startTimeSecondIndex = System.currentTimeMillis();
        //PhoenixSQLOperation.createSecondIndexForGeoNameOfTable();
        //PhoenixSQLOperation.createSecondIndexForGeoPointTableLWD1MLong();
        //PhoenixSQLOperation.createSecondIndexForGeoHashValueBase32OfTable();
        //PhoenixSQLOperation.createSecondIndexForGeoHashValueLongOfTable100M();
//        PhoenixSQLOperation.createSecondIndexHintForGeoHashValueLongOfTable();
//        long endTimeSecondIndex = System.currentTimeMillis();
//        System.out.println("CreateSecondIndex-Time: "+(endTimeSecondIndex - startTimeSecondIndex));

//        long startTimeSecondIndex = System.currentTimeMillis();
//        PhoenixSQLOperation.insert30MRecordToFile();
//        long endTimeSecondIndex = System.currentTimeMillis();
//        System.out.println("Insert30MRecordsToFile-Time: "+(endTimeSecondIndex - startTimeSecondIndex));

        //Phoenix二级索引测试，针对geoHashValueLong列，类型为BigInt
//        long startTimeSelectByGeoID = System.currentTimeMillis();
//        long[] geoHashValueLongs = PhoenixSQLOperation.getGeoHashValueLongsSelectByRandomGeoIDToTestPhoenixSecondIndex();
//        long endTimeSelectByGeoID = System.currentTimeMillis();
//        System.out.println("SelectByGeoID-Time1000: "+(endTimeSelectByGeoID - startTimeSelectByGeoID));
//        long startTimeSelectByGeoHashValueLong = System.currentTimeMillis();
//        PhoenixSQLOperation.selectAndQueryByGeoHashValueLongsToTestPhoenixSecondIndex(geoHashValueLongs);
//        long endTimeSelectByGeoHashValueLong = System.currentTimeMillis();
//        System.out.println("SelectByGeoHashValueLong-Time1000: "+(endTimeSelectByGeoHashValueLong - startTimeSelectByGeoHashValueLong));

        //2、查询操作
        //2.1 19种矩形查询范围的设计
        RectangleQueryScope rQS0_1 = new RectangleQueryScope(10.62569,15.50321,32.62569,30.50321);//第一象限中的矩形范围
        RectangleQueryScope rQS1_1min = new RectangleQueryScope(10.62569,16.50321,11.62569,17.50321);//第一象限中的矩形范围
        RectangleQueryScope rQS2_2 = new RectangleQueryScope(-62.36512,1.25643,-49.25943,32.65124);//第二象限中的矩形范围
        RectangleQueryScope rQS3_2min = new RectangleQueryScope(-62.36512,1.25643,-54.25943,9.65124);//第二象限中的矩形范围
        RectangleQueryScope rQS4_3 = new RectangleQueryScope(-21.62513,-41.58712,-9.25314,-19.25843);//第三象限中的矩形范围
        RectangleQueryScope rQS5_3min = new RectangleQueryScope(-21.62513,-41.58712,-12.25314,-29.25843);//第三象限中的矩形范围
        RectangleQueryScope rQS6_4 = new RectangleQueryScope(29.25876,-52.36914,42.25843,-33.58243);//第四象限中的矩形范围
        RectangleQueryScope rQS7_4min = new RectangleQueryScope(29.25876,-52.36914,37.25843,-44.58243);//第四象限中的矩形范围
        RectangleQueryScope rQS8_12 = new RectangleQueryScope(-16.28412,11.58943,9.25846,35.26849);//横跨第一、二象限的矩形范围
        RectangleQueryScope rQS9_12min = new RectangleQueryScope(-6.28412,11.58943,9.25846,23.26849);//横跨第一、二象限的矩形范围
        RectangleQueryScope rQS10_23 = new RectangleQueryScope(-34.25894,-28.33256,-8.25931,4.26985);//横跨第二、三象限的矩形范围
        RectangleQueryScope rQS11_23min = new RectangleQueryScope(-34.25894,-8.33256,-25.25931,4.26985);//横跨第二、三象限的矩形范围
        RectangleQueryScope rQS12_34 = new RectangleQueryScope(-15.28694,-52.36942,10.25897,-31.25896);//横跨第三、四象限的矩形范围
        RectangleQueryScope rQS13_34min = new RectangleQueryScope(-5.28694,-52.36942,9.25897,-43.25896);//横跨第三、四象限的矩形范围
        RectangleQueryScope rQS14_41 = new RectangleQueryScope(48.23654,-19.28654,64.25987,13.25897);//横跨第四、一象限的矩形范围
        RectangleQueryScope rQS15_41min = new RectangleQueryScope(48.23654,-9.28654,57.25987,3.25897);//横跨第四、一象限的矩形范围
        RectangleQueryScope rQS16_1234 = new RectangleQueryScope(-14.22314,-25.18972,29.55846,18.22579);//横跨第一二三四象限的矩形范围
        RectangleQueryScope rQS17_1234min = new RectangleQueryScope(-4.22314,-5.18972,5.55846,4.22579);//横跨第一二三四象限的矩形范围
        RectangleQueryScope rQS18_1234min2 = new RectangleQueryScope(-4.22314,-5.18972,2.55846,1.22579);//横跨第一二三四象限的矩形范围
        //创建通用的查询范围对象
        RectangleQueryScope r = new RectangleQueryScope();
//        r = rQS1_1min;
        double xLongitudeBLFly = -160.23541;
        //double yLatitudeBLFly = -85.25489;
        double deltaXFly = 0.5;
        double deltaYFly = 0.25;
//        RectangleQueryScope rQSFly = new RectangleQueryScope();
//        rQSFly.RectangleQueryScopeDelta(xLongitudeBLFly,yLatitudeBLFly,deltaXFly,deltaYFly);
        int count1 = 0;
        for(int a=0;a<10;a++){
            xLongitudeBLFly+=8;
            double yLatitudeBLFly = -85.25489;
            for(int b=0;b<11;b++){
                yLatitudeBLFly+=5;
                RectangleQueryScope rQSFly = new RectangleQueryScope();
                rQSFly.RectangleQueryScopeDelta(xLongitudeBLFly,yLatitudeBLFly,deltaXFly,deltaYFly);
                int count =0;
                for (double areaRatioTemp = 2.0; areaRatioTemp <= 2.0; areaRatioTemp+=0.1) {
                    count++;
                    DecimalFormat df = new DecimalFormat("#.0000");
                    double areaRatio = Double.parseDouble(df.format(areaRatioTemp));
                    System.out.println("---------------------------"+count1+"--------------------------------------");
                    long startTimeQueryWithGeoHashAndSecondFiltering10M = System.currentTimeMillis();
                    ArrayList<GeoPointTableRecordSimple> gPTRWithGeoHashAndSecondFiltering10M =
                            PhoenixSQLOperation.selectAndQueryRecordsWithGeoHashIndexAreaRatioAndSecondFilteringUnionAllFrom10MTable(rQSFly, areaRatio);
                    long endTimeQueryWithGeoHashAndSecondFiltering10M = System.currentTimeMillis();
                    //2.4.2 SQL-GeoHash的BetweenAnd与直接判断的UnionAll
                    long startTimeQueryWithGeoHashAndDirectJudge10M = System.currentTimeMillis();
                    ArrayList<GeoPointTableRecordSimple> gPTRWithGeoHashAndDirectJudge10M =
                            PhoenixSQLOperation.selectAndQueryRecordsWithGeoHashIndexAreaRatioAndDirectJudgeUnionAllFrom10MTable(rQSFly, areaRatio);
                    long endTimeQueryWithGeoHashAndDirectJudge10M = System.currentTimeMillis();
                    //2.4.3 SQL-GeoHash的BetweenAnd与UDF函数的UnionAll
                    long startTimeQueryWithGeoHashAndUDFFunction10M = System.currentTimeMillis();
                    ArrayList<GeoPointTableRecordSimple> gPTRWithGeoHashAndUDFFunction10M =
                        PhoenixSQLOperation.selectAndQueryRecordsWithGeoHashIndexAreaRatioAndUDFFunctionUnionAllFrom10MTable(rQSFly, areaRatio);
                    long endTimeQueryWithGeoHashAndUDFFunction10M = System.currentTimeMillis();
                    //获得GeoHash段集合并计算个数，用于后面打印显示
                    Stack<long[]> gGeoHashLongs =
                            GeoHashConversion.getMergedGeoHashLongsByGeoHashIndexAlgorithmWithBFSAndAreaRatio(rQSFly, areaRatio);
                    //for()
                    //相关结果输出
                    System.out.println("RectangleQueryScope: " + rQSFly.toString());
                    System.out.println("GeoHashIndexAlgorithm-AreaAndSearchTime: " + count
                            + "#" + (endTimeQueryWithGeoHashAndSecondFiltering10M - startTimeQueryWithGeoHashAndSecondFiltering10M) / 1000.0
                            + "%" + (endTimeQueryWithGeoHashAndDirectJudge10M - startTimeQueryWithGeoHashAndDirectJudge10M) / 1000.0
                            + "%" + (endTimeQueryWithGeoHashAndUDFFunction10M - startTimeQueryWithGeoHashAndUDFFunction10M) / 1000.0);
                    //+ "%" + (endTimeQueryWithGeoHashAndUDFFunctionNew - startTimeQueryWithGeoHashAndUDFFunctionNew) / 1000.0);
                    System.out.println("GeoHashLongsAndRectangleRangeQueryWithIndex-Size: "
                            + gGeoHashLongs.size() + "#" + gPTRWithGeoHashAndSecondFiltering10M.size()
                            + "%" + gPTRWithGeoHashAndDirectJudge10M.size() + "%" + gPTRWithGeoHashAndUDFFunction10M.size());
//////                //相关结果写入文件操作，便于MatLab画图
                String strSDTGeoHashThreeAll = count + " "
                        + ((endTimeQueryWithGeoHashAndSecondFiltering10M - startTimeQueryWithGeoHashAndSecondFiltering10M) / 1000.0 + " ")
                        + ((endTimeQueryWithGeoHashAndDirectJudge10M - startTimeQueryWithGeoHashAndDirectJudge10M) / 1000.0 + " ")
                        + ((endTimeQueryWithGeoHashAndUDFFunction10M - startTimeQueryWithGeoHashAndUDFFunction10M) / 1000.0) + "\n";
                File fileSDTGeoHashThreeAll = new File("rQSFlySDTGeoHashSDU900SumMergeAreaRatio30M20.txt");
                FileUtil.writeToFile(fileSDTGeoHashThreeAll, strSDTGeoHashThreeAll);
                }
                count1++;
            }
        }
//        for(int j=0;j<1000;j++) {
//            System.out.println("================================" + "rQS1_1min" + "=====================================");
            //2.1 无索引的范围查询，遍历所有记录，复杂度为O(n)
//        long startTimeQueryWithoutIndex = System.currentTimeMillis();
//        ArrayList<GeoPointTableRecord> geoPointTableRecordsWithoutIndex
//                = PhoenixSQLOperation.selectAndQueryRecordsWithoutGeoHashIndex(r);
//        long endTimeQueryWithoutIndex = System.currentTimeMillis();
//        //for(GeoPointTableRecord g:geoPointTableRecordsWithoutIndex){
//          //System.out.println(g.toString());
//        //}
//        System.out.println("RectangleQueryScope: " + r.toString());
//        System.out.println("RectangleRangeQueryWithoutIndex-SizeAndTime: "+geoPointTableRecordsWithoutIndex.size()
//                +"#"+(endTimeQueryWithoutIndex - startTimeQueryWithoutIndex)/1000.0);
//        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
            //2.2 SQL+直接查询，全表扫描，遍历所有记录，
//        long startTimeQueryWithDirectJudge = System.currentTimeMillis();
//        ArrayList<GeoPointTableRecord> gPTRWithDirectJudge = PhoenixSQLOperation.selectAndQueryRecordsWithDirectJudge(r);
//        long endTimeQueryWithDirectJudge = System.currentTimeMillis();
//        System.out.println("RectangleQueryScope: " + r.toString());
//        System.out.println("RectangleRangeQueryWithIndex-SizeAndTime:" + gPTRWithDirectJudge.size()
//                + "#" + (endTimeQueryWithDirectJudge - startTimeQueryWithDirectJudge) / 1000.0);
//        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        //=============================10M数据表的操作====================================
//        long startTimeQueryWithDirectJudge10M = System.currentTimeMillis();
//        ArrayList<GeoPointTableRecordSimple> gPTRWithDirectJudge10M = PhoenixSQLOperation.selectAndQueryRecordsWithDirectJudgeFrom10MTable(r);
//        long endTimeQueryWithDirectJudge10M = System.currentTimeMillis();
//        System.out.println("RectangleQueryScope: " + r.toString());
//        System.out.println("RectangleRangeQueryWithIndex-SizeAndTime:" + gPTRWithDirectJudge10M.size()
//                + "#" + (endTimeQueryWithDirectJudge10M - startTimeQueryWithDirectJudge10M) / 1000.0);
//        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
//            //2.3 SQL+UDF函数查询，全表扫描，遍历所有记录
////        long startTimeQueryWithUDFFunction = System.currentTimeMillis();
////        ArrayList<GeoPointTableRecord> gPTRWithUDFFunction = PhoenixSQLOperation.selectAndQueryRecordsWithUDFFunction(r);
////        long endTimeQueryWithUDFFunction = System.currentTimeMillis();
////        System.out.println("RectangleQueryScope: "+r.toString());
////        System.out.println("RectangleRangeQueryWithIndex-SizeAndTime:"+gPTRWithUDFFunction.size()
////               +"#"+(endTimeQueryWithUDFFunction - startTimeQueryWithUDFFunction)/1000.0);
//            //2.4GeoHash查询，三种情况，改变搜索深度，进行查询
//            int count =0;
//            for (double areaRatioTemp = 2.5; areaRatioTemp <= 2.5; areaRatioTemp+=0.1) {
//                count++;
//                DecimalFormat df = new DecimalFormat("#.0000");
//                double areaRatio = Double.parseDouble(df.format(areaRatioTemp));
//                System.out.println("-----------------------------------------------------------------");
////
////                //2.4.1 SQL-GeoHash的BetweenAnd的UnionAll+本地内存二次过滤
////                long startTimeQueryWithGeoHashAndSecondFiltering = System.currentTimeMillis();
////                ArrayList<GeoPointTableRecord> gPTRWithGeoHashAndSecondFiltering =
////                        PhoenixSQLOperation.selectAndQueryRecordsWithGeoHashIndexAndSecondFilteringUnionAll(r, areaRatio);
////                long endTimeQueryWithGeoHashAndSecondFiltering = System.currentTimeMillis();
////                //2.4.2 SQL-GeoHash的BetweenAnd与直接判断的UnionAll
////                long startTimeQueryWithGeoHashAndDirectJudge = System.currentTimeMillis();
////                ArrayList<GeoPointTableRecord> gPTRWithGeoHashAndDirectJudge =
////                        PhoenixSQLOperation.selectAndQueryRecordsWithGeoHashIndexAndDirectJudgeUnionAll(r, areaRatio);
////                long endTimeQueryWithGeoHashAndDirectJudge = System.currentTimeMillis();
////                //2.4.3 SQL-GeoHash的BetweenAnd与UDF函数的UnionAll
////                long startTimeQueryWithGeoHashAndUDFFunction = System.currentTimeMillis();
////                ArrayList<GeoPointTableRecord> gPTRWithGeoHashAndUDFFunction =
////                        PhoenixSQLOperation.selectAndQueryRecordsWithGeoHashIndexAndUDFFunctionUnionAll(r, areaRatio);
////                long endTimeQueryWithGeoHashAndUDFFunction = System.currentTimeMillis();
//////                long startTimeQueryWithGeoHashAndUDFFunctionNew = System.currentTimeMillis();
//////                ArrayList<GeoPointTableRecord> gPTRWithGeoHashAndUDFFunctionNew =
//////                        PhoenixSQLOperation.selectAndQueryRecordsWithGeoHashIndexAndUDFFunctionUnionAllNew(r, searchDepthManual);
//////                long endTimeQueryWithGeoHashAndUDFFunctionNew = System.currentTimeMillis();
////                //获得GeoHash段集合并计算个数，用于后面打印显示
////                Stack<long[]> gGeoHashLongs =
////                        GeoHashConversion.getMergedGeoHashLongsByGeoHashIndexAlgorithmWithBFSAndAreaRatio(r, areaRatio);
////                //相关结果输出
////                System.out.println("RectangleQueryScope: " + r.toString());
////                System.out.println("GeoHashIndexAlgorithm-AreaAndSearchTime: "
////                        + "#" + (endTimeQueryWithGeoHashAndSecondFiltering - startTimeQueryWithGeoHashAndSecondFiltering) / 1000.0
////                        + "%" + (endTimeQueryWithGeoHashAndDirectJudge - startTimeQueryWithGeoHashAndDirectJudge) / 1000.0
////                        + "%" + (endTimeQueryWithGeoHashAndUDFFunction - startTimeQueryWithGeoHashAndUDFFunction) / 1000.0);
////                        //+ "%" + (endTimeQueryWithGeoHashAndUDFFunctionNew - startTimeQueryWithGeoHashAndUDFFunctionNew) / 1000.0);
////                System.out.println("GeoHashLongsAndRectangleRangeQueryWithIndex-Size: "
////                        + gGeoHashLongs.size() + "#" + gPTRWithGeoHashAndSecondFiltering.size()
////                        + "%" + gPTRWithGeoHashAndDirectJudge.size() + "%" + gPTRWithGeoHashAndUDFFunction.size());
//////                //相关结果写入文件操作，便于MatLab画图
////                String strSDTGeoHashThreeAll = count + " "
////                        + ((endTimeQueryWithGeoHashAndSecondFiltering - startTimeQueryWithGeoHashAndSecondFiltering) / 1000.0 + " ")
////                        + ((endTimeQueryWithGeoHashAndDirectJudge - startTimeQueryWithGeoHashAndDirectJudge) / 1000.0 + " ")
////                        + ((endTimeQueryWithGeoHashAndUDFFunction - startTimeQueryWithGeoHashAndUDFFunction) / 1000.0) + "\n";
////                File fileSDTGeoHashThreeAll = new File("rQS5_3minSDTGeoHashSDU20SumMergeAreaRatio.txt");
////                FileUtil.writeToFile(fileSDTGeoHashThreeAll, strSDTGeoHashThreeAll);
//                //=============================10M数据表的操作====================================
//                long startTimeQueryWithGeoHashAndSecondFiltering10M = System.currentTimeMillis();
//                ArrayList<GeoPointTableRecordSimple> gPTRWithGeoHashAndSecondFiltering10M =
//                        PhoenixSQLOperation.selectAndQueryRecordsWithGeoHashIndexAreaRatioAndSecondFilteringUnionAllFrom10MTable(r, areaRatio);
//                long endTimeQueryWithGeoHashAndSecondFiltering10M = System.currentTimeMillis();
//                //2.4.2 SQL-GeoHash的BetweenAnd与直接判断的UnionAll
//                long startTimeQueryWithGeoHashAndDirectJudge10M = System.currentTimeMillis();
//                ArrayList<GeoPointTableRecordSimple> gPTRWithGeoHashAndDirectJudge10M =
//                        PhoenixSQLOperation.selectAndQueryRecordsWithGeoHashIndexAreaRatioAndDirectJudgeUnionAllFrom10MTable(r, areaRatio);
//                long endTimeQueryWithGeoHashAndDirectJudge10M = System.currentTimeMillis();
//                //2.4.3 SQL-GeoHash的BetweenAnd与UDF函数的UnionAll
//                long startTimeQueryWithGeoHashAndUDFFunction10M = System.currentTimeMillis();
////                ArrayList<GeoPointTableRecordSimple> gPTRWithGeoHashAndUDFFunction10M =
////                        PhoenixSQLOperation.selectAndQueryRecordsWithGeoHashIndexAreaRatioAndUDFFunctionUnionAllFrom10MTable(r, areaRatio);
//                long endTimeQueryWithGeoHashAndUDFFunction10M = System.currentTimeMillis();
////                long startTimeQueryWithGeoHashAndUDFFunctionNew = System.currentTimeMillis();
////                ArrayList<GeoPointTableRecord> gPTRWithGeoHashAndUDFFunctionNew =
////                        PhoenixSQLOperation.selectAndQueryRecordsWithGeoHashIndexAndUDFFunctionUnionAllNew(r, searchDepthManual);
////                long endTimeQueryWithGeoHashAndUDFFunctionNew = System.currentTimeMillis();
//                //获得GeoHash段集合并计算个数，用于后面打印显示
//                Stack<long[]> gGeoHashLongs =
//                        GeoHashConversion.getMergedGeoHashLongsByGeoHashIndexAlgorithmWithBFSAndAreaRatio(r, areaRatio);
//                //for()
//                //相关结果输出
//                System.out.println("RectangleQueryScope: " + r.toString());
//                System.out.println("GeoHashIndexAlgorithm-AreaAndSearchTime: " + count
//                        + "#" + (endTimeQueryWithGeoHashAndSecondFiltering10M - startTimeQueryWithGeoHashAndSecondFiltering10M) / 1000.0
//                        + "%" + (endTimeQueryWithGeoHashAndDirectJudge10M - startTimeQueryWithGeoHashAndDirectJudge10M) / 1000.0
//                        + "%" + (endTimeQueryWithGeoHashAndUDFFunction10M - startTimeQueryWithGeoHashAndUDFFunction10M) / 1000.0);
//                //+ "%" + (endTimeQueryWithGeoHashAndUDFFunctionNew - startTimeQueryWithGeoHashAndUDFFunctionNew) / 1000.0);
//                System.out.println("GeoHashLongsAndRectangleRangeQueryWithIndex-Size: "
//                        + gGeoHashLongs.size() + "#" + gPTRWithGeoHashAndSecondFiltering10M.size()
//                        + "%" + gPTRWithGeoHashAndDirectJudge10M.size());// + "%" + gPTRWithGeoHashAndUDFFunction10M.size());
////////                //相关结果写入文件操作，便于MatLab画图
////                String strSDTGeoHashThreeAll = count + " "
////                        + ((endTimeQueryWithGeoHashAndSecondFiltering10M - startTimeQueryWithGeoHashAndSecondFiltering10M) / 1000.0 + " ")
////                        + ((endTimeQueryWithGeoHashAndDirectJudge10M - startTimeQueryWithGeoHashAndDirectJudge10M) / 1000.0 + " ")
////                        + ((endTimeQueryWithGeoHashAndUDFFunction10M - startTimeQueryWithGeoHashAndUDFFunction10M) / 1000.0) + "\n";
////                File fileSDTGeoHashThreeAll = new File("rQS1_1minSDTGeoHashSDU100SumMergeAreaRatio10M.txt");
////                FileUtil.writeToFile(fileSDTGeoHashThreeAll, strSDTGeoHashThreeAll);
//            }
        //}
//
//
//        //PhoenixSecondTest
////        long startTimeSelectByGeoID = System.currentTimeMillis();
////        ArrayList<String> geoNames = PhoenixSQLOperation.getGeoNamesSelectByRandomGeoIDToTestPhoenixSecondIndex();
////        long endTimeSelectByGeoID = System.currentTimeMillis();
////        System.out.println("SelectByGeoID-Time100: "+(endTimeSelectByGeoID - startTimeSelectByGeoID));
////        long startTimeSelectByGeoName = System.currentTimeMillis();
////        PhoenixSQLOperation.selectAndQueryByGeoNamesToTestPhoenixSecondIndex(geoNames);
////        long endTimeSelectByGeoName = System.currentTimeMillis();
////        System.out.println("SelectByGeoName-Time100: "+(endTimeSelectByGeoName - startTimeSelectByGeoName));
//
////        long startTimeSelectByGeoID = System.currentTimeMillis();
////        ArrayList<String> geoHashValues = PhoenixSQLOperation.getGeoHashValuesSelectByRandomGeoIDToTestPhoenixSecondIndex();
////        long endTimeSelectByGeoID = System.currentTimeMillis();
////        System.out.println("SelectByGeoID-Time100: "+(endTimeSelectByGeoID - startTimeSelectByGeoID));
////        long startTimeSelectByGeoName = System.currentTimeMillis();
////        PhoenixSQLOperation.selectAndQueryByGeoHashValuesToTestPhoenixSecondIndex(geoHashValues);
////        long endTimeSelectByGeoName = System.currentTimeMillis();
////        System.out.println("SelectByGeoName-Time100: "+(endTimeSelectByGeoName - startTimeSelectByGeoName));
//
////     //Phoenix二级索引测试，针对geoHashValueLong列，类型为BigInt
////     long startTimeSelectByGeoID = System.currentTimeMillis();
////     long[] geoHashValueLongs = PhoenixSQLOperation.getGeoHashValueLongsSelectByRandomGeoIDToTestPhoenixSecondIndex();
////     long endTimeSelectByGeoID = System.currentTimeMillis();
////     System.out.println("SelectByGeoID-Time100: "+(endTimeSelectByGeoID - startTimeSelectByGeoID));
////     long startTimeSelectByGeoHashValueLong = System.currentTimeMillis();
////     PhoenixSQLOperation.selectAndQueryByGeoHashValueLongsToTestPhoenixSecondIndex(geoHashValueLongs);
////     long endTimeSelectByGeoHashValueLong = System.currentTimeMillis();
////     System.out.println("SelectByGeoHashValueLong-Time100: "+(endTimeSelectByGeoHashValueLong - startTimeSelectByGeoHashValueLong));
//
//
//        //GeoHash叶节点合并测试
////        for(int searchManualTestMerge=1;searchManualTestMerge<=10;searchManualTestMerge++){
////            System.out.println("================="+"SearchDepthManual: "+searchManualTestMerge+"===============");
////            long startTimeGeoHashLongs = System.currentTimeMillis();
////            Stack<long[]> gGeoHashLongsMerge =
////                    GeoHashConversion.rangeQueryWithGeoHashIndexAccordingToRectangleQueryScoreLeafMerge(r, searchManualTestMerge);
////            long endTimeGeoHashLongs = System.currentTimeMillis();
////            long startTimeGeoHashLongsMerge = System.currentTimeMillis();
////            Stack<long[]> gGeoHashLongsMergeTest =
////                    GeoHashConversion.rangeQueryWithGeoHashIndexAccordingToRectangleQueryScoreLeafMergeTest(r,searchManualTestMerge);
////            long endTimeGeoHashLongsMerge = System.currentTimeMillis();
////            System.out.println("GetGeoHashLongsAndMerge-Time: "+(endTimeGeoHashLongs-startTimeGeoHashLongs)
////                    +"#"+(endTimeGeoHashLongsMerge-startTimeGeoHashLongsMerge));
////            System.out.println("GetGeoHashLongsAndMerge-Size: "+gGeoHashLongsMerge.size() +"#"+gGeoHashLongsMergeTest.size());
//////            for(long[] g:gGeoHashLongsMerge){
//////                System.out.println(g[0]+"#"+g[1]);
//////            }
//////            System.out.println("------------------------------------------");
//////            for(long[] gm:gGeoHashLongsMergeTest){
//////                System.out.println(gm[0]+"#"+gm[1]);
//////            }
//////
////        }

        //GeoHash索引算法修订测试，时间：2016年9月30日10:03:35
//        for(int i=0;i<1000;i++) {
//            long startTimeTest = System.currentTimeMillis();
//            Stack<GeoHashIndexRecord> gHIRArray = GeoHashConversion.getMergedGeoHashLongsByGeoHashIndexAlgorithmWithBFSAndAreaRatioLWD(r);
//            long endTimeTest = System.currentTimeMillis();
//            for (GeoHashIndexRecord g : gHIRArray) {
//                long startTimeQueryWithGeoHashAndSecondFiltering10M = System.currentTimeMillis();
//                ArrayList<GeoPointTableRecordSimple> gPTRWithGeoHashAndSecondFiltering10M =
//                        PhoenixSQLOperation.selectAndQueryRecordsWithGeoHashIndexAreaRatioAndSecondFilteringUnionAllFrom10MTableTest(g.sGeoHashLongs, r);
//                long endTimeQueryWithGeoHashAndSecondFiltering10M = System.currentTimeMillis();
//                //2.4.2 SQL-GeoHash的BetweenAnd与直接判断的UnionAll
//                long startTimeQueryWithGeoHashAndDirectJudge10M = System.currentTimeMillis();
//                ArrayList<GeoPointTableRecordSimple> gPTRWithGeoHashAndDirectJudge10M =
//                        PhoenixSQLOperation.selectAndQueryRecordsWithGeoHashIndexAreaRatioAndDirectJudgeUnionAllFrom10MTableTest(g.sGeoHashLongs, r);
//                long endTimeQueryWithGeoHashAndDirectJudge10M = System.currentTimeMillis();
//                //2.4.3 SQL-GeoHash的BetweenAnd与UDF函数的UnionAll
//                long startTimeQueryWithGeoHashAndUDFFunction10M = System.currentTimeMillis();
//                ArrayList<GeoPointTableRecordSimple> gPTRWithGeoHashAndUDFFunction10M =
//                        PhoenixSQLOperation.selectAndQueryRecordsWithGeoHashIndexAreaRatioAndUDFFunctionUnionAllFrom10MTableTest(g.sGeoHashLongs, r);
//                long endTimeQueryWithGeoHashAndUDFFunction10M = System.currentTimeMillis();
//
//                System.out.println("RectangleQueryScope: " + r.toString());
//                System.out.println("GeoHashIndexAlgorithm-AreaAndSearchTime: " + g.searchDepth
//                        + "#" + (endTimeQueryWithGeoHashAndSecondFiltering10M - startTimeQueryWithGeoHashAndSecondFiltering10M) / 1000.0
//                        + "%" + (endTimeQueryWithGeoHashAndDirectJudge10M - startTimeQueryWithGeoHashAndDirectJudge10M) / 1000.0
//                        + "%" + (endTimeQueryWithGeoHashAndUDFFunction10M - startTimeQueryWithGeoHashAndUDFFunction10M) / 1000.0);
//                System.out.println("GeoHashLongsAndRectangleRangeQueryWithIndex-Size: "
//                        + g.sizeOfGeoHashLongs + "#" + gPTRWithGeoHashAndSecondFiltering10M.size()
//                        + "%" + gPTRWithGeoHashAndDirectJudge10M.size() + "%" + gPTRWithGeoHashAndUDFFunction10M.size());
//                //System.out.println(g.toString());
//                String strSDTGeoHashThreeAll = g.searchDepth + " "
//                        + ((endTimeQueryWithGeoHashAndSecondFiltering10M - startTimeQueryWithGeoHashAndSecondFiltering10M) / 1000.0 + " ")
//                        + ((endTimeQueryWithGeoHashAndDirectJudge10M - startTimeQueryWithGeoHashAndDirectJudge10M) / 1000.0 + " ")
//                        + ((endTimeQueryWithGeoHashAndUDFFunction10M - startTimeQueryWithGeoHashAndUDFFunction10M) / 1000.0) + "\n";
//                File fileSDTGeoHashThreeAll = new File("rQS1_1minSDTGeoHashSDU1000SumMergeAreaRatio10MNew.txt");
//                FileUtil.writeToFile(fileSDTGeoHashThreeAll, strSDTGeoHashThreeAll);
//            }
//
//            System.out.println("Size: " + gHIRArray.size());
//            System.out.println("Time: " +. (endTimeTest - startTimeTest));
//        }

        PhoenixSQLOperation.closeConnectionWithHBase();


    }
}
