package cc.xidian.MainTest;

import cc.xidian.GeoHash.GeoHashConversion;
import cc.xidian.GeoHash.RectanglePrefix;
import cc.xidian.GeoHash.RectangleStrBinaryPrefix;
import cc.xidian.GeoObject.GeoPointTableRecord;
import cc.xidian.GeoObject.RectangleQueryScope;
import cc.xidian.PhoenixOperation.PhoenixSQLOperation;
import cc.xidian.geoUtil.FileUtil;

import java.io.File;
import java.lang.reflect.Array;
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
        //1.1 创建表并插入数据操作，该操作只执行一次
//        long startTimeCreateAndInsertRecords = System.currentTimeMillis();
//        PhoenixSQLOperation.createAndInsertRecordToTableNamedGeoPointTable10M();//只执行一次
//        long endTimeCreateAndInsertRecords = System.currentTimeMillis();
//        System.out.println("CreateAndInsertRecords-Time: "+(endTimeCreateAndInsertRecords - startTimeCreateAndInsertRecords));
        //1.2 删除表操作，该操作只执行一次
        //PhoenixSQLOperation.dropTableNamedGeoPointTable();//只执行一次
        //1.3 查询并写入文件操作，该操作只执行一次，目的是获取表中部分数据并进行查看
        //PhoenixSQLOperation.selectResultsToFile();//只执行一次
        //1.4 删除Phoenix二级索引操作，该操作只执行一次
        //PhoenixSQLOperation.dropIndexOfName();//删除二级索引，该操作只执行一次
        //1.5 创建二级索引操作，并计时，该操作只执行一次
//        long startTimeSecondIndex = System.currentTimeMillis();
//        //PhoenixSQLOperation.createSecondIndexForGeoNameOfTable();
//        //PhoenixSQLOperation.createSecondIndexForGeoPointTableLWD1MLong();
//        //PhoenixSQLOperation.createSecondIndexForGeoHashValueBase32OfTable();
//        PhoenixSQLOperation.createSecondIndexForGeoHashValueLongOfTable10M();
//        long endTimeSecondIndex = System.currentTimeMillis();
//        System.out.println("CreateSecondIndex-Time: "+(endTimeSecondIndex - startTimeSecondIndex));

        //2、查询操作
        //2.1 19种矩形查询范围的设计
        RectangleQueryScope rQS0_1 = new RectangleQueryScope(10.62569,15.50321,32.62569,30.50321);//第一象限中的矩形范围
        RectangleQueryScope rQS1_1min = new RectangleQueryScope(10.62569,16.50321,13.62569,19.50321);//第一象限中的矩形范围
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
        r = rQS0_1;
        //for(int j=0;j<20;j++) {
            System.out.println("================================" + "rQS0_1" + "=====================================");
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
//            long startTimeQueryWithDirectJudge = System.currentTimeMillis();
//            ArrayList<GeoPointTableRecord> gPTRWithDirectJudge = PhoenixSQLOperation.selectAndQueryRecordsWithDirectJudge(r);
//            long endTimeQueryWithDirectJudge = System.currentTimeMillis();
//            System.out.println("RectangleQueryScope: " + r.toString());
//            System.out.println("RectangleRangeQueryWithIndex-SizeAndTime:" + gPTRWithDirectJudge.size()
//                    + "#" + (endTimeQueryWithDirectJudge - startTimeQueryWithDirectJudge) / 1000.0);
//            System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
            //2.3 SQL+UDF函数查询，全表扫描，遍历所有记录
//        long startTimeQueryWithUDFFunction = System.currentTimeMillis();
//        ArrayList<GeoPointTableRecord> gPTRWithUDFFunction = PhoenixSQLOperation.selectAndQueryRecordsWithUDFFunction(r);
//        long endTimeQueryWithUDFFunction = System.currentTimeMillis();
//        System.out.println("RectangleQueryScope: "+r.toString());
//        System.out.println("RectangleRangeQueryWithIndex-SizeAndTime:"+gPTRWithUDFFunction.size()
//               +"#"+(endTimeQueryWithUDFFunction - startTimeQueryWithUDFFunction)/1000.0);
            //2.4GeoHash查询，三种情况，改变搜索深度，进行查询

//            for (int searchDepthManual = 1; searchDepthManual <= 20; searchDepthManual++) {
//                System.out.println("-----------------------------------------------------------------");
//                //2.4.0 查询测试
////            long startTimeQueryWithGeoHashAndSecondFiltering = System.currentTimeMillis();
////            ArrayList<GeoPointTableRecord> gPTRWithGeoHashAndSecondFiltering =
////                    PhoenixSQLOperation.selectAndQueryRecordsWithGeoHashIndexAndUDFFunctionUnionAll(r, searchDepthManual);
////            long endTimeQueryWithGeoHashAndSecondFiltering = System.currentTimeMillis();
////            ArrayList<long[]> gGeoHashLongs =
////                    GeoHashConversion.rangeQueryWithGeoHashIndexAccordingToRectangleQueryScore(r, searchDepthManual);
////            System.out.println("RectangleQueryScope: " + r.toString());
////            System.out.println("GeoHashIndexAlgorithm-SearchDepthAndSearchTime: " + searchDepthManual
////                    + "#" + (endTimeQueryWithGeoHashAndSecondFiltering - startTimeQueryWithGeoHashAndSecondFiltering) / 1000.0);
////            System.out.println("GeoHashLongsAndRectangleRangeQueryWithIndex-Size: "
////                    + gGeoHashLongs.size() + "#" + gPTRWithGeoHashAndSecondFiltering.size());
                //2.4.1 SQL-GeoHash的BetweenAnd的UnionAll+本地内存二次过滤
//                long startTimeQueryWithGeoHashAndSecondFiltering = System.currentTimeMillis();
//                ArrayList<GeoPointTableRecord> gPTRWithGeoHashAndSecondFiltering =
//                        PhoenixSQLOperation.selectAndQueryRecordsWithGeoHashIndexAndSecondFilteringUnionAll(r, searchDepthManual);
//                long endTimeQueryWithGeoHashAndSecondFiltering = System.currentTimeMillis();
//                //2.4.2 SQL-GeoHash的BetweenAnd与直接判断的UnionAll
//                long startTimeQueryWithGeoHashAndDirectJudge = System.currentTimeMillis();
//                ArrayList<GeoPointTableRecord> gPTRWithGeoHashAndDirectJudge =
//                        PhoenixSQLOperation.selectAndQueryRecordsWithGeoHashIndexAndDirectJudgeUnionAll(r, searchDepthManual);
//                long endTimeQueryWithGeoHashAndDirectJudge = System.currentTimeMillis();
//                //2.4.3 SQL-GeoHash的BetweenAnd与UDF函数的UnionAll
//                long startTimeQueryWithGeoHashAndUDFFunction = System.currentTimeMillis();
//                ArrayList<GeoPointTableRecord> gPTRWithGeoHashAndUDFFunction =
//                        PhoenixSQLOperation.selectAndQueryRecordsWithGeoHashIndexAndUDFFunctionUnionAll(r, searchDepthManual);
//                long endTimeQueryWithGeoHashAndUDFFunction = System.currentTimeMillis();
//                //获得GeoHash段集合并计算个数，用于后面打印显示
//                ArrayList<long[]> gGeoHashLongs =
//                        GeoHashConversion.rangeQueryWithGeoHashIndexAccordingToRectangleQueryScore(r, searchDepthManual);
//                //相关结果输出
//                System.out.println("RectangleQueryScope: " + r.toString());
//                System.out.println("GeoHashIndexAlgorithm-SearchDepthAndSearchTime: " + searchDepthManual
//                        + "#" + (endTimeQueryWithGeoHashAndSecondFiltering - startTimeQueryWithGeoHashAndSecondFiltering) / 1000.0
//                        + "%" + (endTimeQueryWithGeoHashAndDirectJudge - startTimeQueryWithGeoHashAndDirectJudge) / 1000.0
//                        + "%" + (endTimeQueryWithGeoHashAndUDFFunction - startTimeQueryWithGeoHashAndUDFFunction) / 1000.0);
//                System.out.println("GeoHashLongsAndRectangleRangeQueryWithIndex-Size: "
//                        + gGeoHashLongs.size() + "#" + gPTRWithGeoHashAndSecondFiltering.size()
//                        + "%" + gPTRWithGeoHashAndDirectJudge.size() + "%" + gPTRWithGeoHashAndUDFFunction.size());
////                //相关结果写入文件操作，便于MatLab画图
//                String strSDTGeoHashThreeAll = searchDepthManual + " "
//                        + ((endTimeQueryWithGeoHashAndSecondFiltering - startTimeQueryWithGeoHashAndSecondFiltering) / 1000.0 + " ")
//                        + ((endTimeQueryWithGeoHashAndDirectJudge - startTimeQueryWithGeoHashAndDirectJudge) / 1000.0 + " ")
//                        + ((endTimeQueryWithGeoHashAndUDFFunction - startTimeQueryWithGeoHashAndUDFFunction) / 1000.0) + "\n";
//                File fileSDTGeoHashThreeAll = new File("rQS5_3minSDTGeoHashSDU20SumMerge.txt");
//                FileUtil.writeToFile(fileSDTGeoHashThreeAll, strSDTGeoHashThreeAll);
//            }
      //}

        //GeoHash编码转换正确，测试完成
//        long geoHashZY = GeoHashConversion.LongLatToHash(-50.0,35.0);
//        System.out.println(geoHashZY);
//        double[] lonLatZY = GeoHashConversion.HashToLongLat(geoHashZY);
//        System.out.println(lonLatZY[0]+"#"+lonLatZY[1]);

        //我的转换方法也正确，张洋的正确
//        long geoHashLWD = GeoHashConversion.encodeGeoHashFromLonAndLatLong(-50.0,30.0);
//        System.out.println(geoHashLWD);
//        double[] lonLatLWD = GeoHashConversion.decodeLonAndLatFromGeoHashLong(geoHashZY);
//        System.out.println(lonLatLWD[0]+"#"+lonLatLWD[1]);


        //PhoenixSecondTest
//        long startTimeSelectByGeoID = System.currentTimeMillis();
//        ArrayList<String> geoNames = PhoenixSQLOperation.getGeoNamesSelectByRandomGeoIDToTestPhoenixSecondIndex();
//        long endTimeSelectByGeoID = System.currentTimeMillis();
//        System.out.println("SelectByGeoID-Time100: "+(endTimeSelectByGeoID - startTimeSelectByGeoID));
//        long startTimeSelectByGeoName = System.currentTimeMillis();
//        PhoenixSQLOperation.selectAndQueryByGeoNamesToTestPhoenixSecondIndex(geoNames);
//        long endTimeSelectByGeoName = System.currentTimeMillis();
//        System.out.println("SelectByGeoName-Time100: "+(endTimeSelectByGeoName - startTimeSelectByGeoName));

//        long startTimeSelectByGeoID = System.currentTimeMillis();
//        ArrayList<String> geoHashValues = PhoenixSQLOperation.getGeoHashValuesSelectByRandomGeoIDToTestPhoenixSecondIndex();
//        long endTimeSelectByGeoID = System.currentTimeMillis();
//        System.out.println("SelectByGeoID-Time100: "+(endTimeSelectByGeoID - startTimeSelectByGeoID));
//        long startTimeSelectByGeoName = System.currentTimeMillis();
//        PhoenixSQLOperation.selectAndQueryByGeoHashValuesToTestPhoenixSecondIndex(geoHashValues);
//        long endTimeSelectByGeoName = System.currentTimeMillis();
//        System.out.println("SelectByGeoName-Time100: "+(endTimeSelectByGeoName - startTimeSelectByGeoName));

     //Phoenix二级索引测试，针对geoHashValueLong列，类型为BigInt
//     long startTimeSelectByGeoID = System.currentTimeMillis();
//     long[] geoHashValueLongs = PhoenixSQLOperation.getGeoHashValueLongsSelectByRandomGeoIDToTestPhoenixSecondIndex();
//     long endTimeSelectByGeoID = System.currentTimeMillis();
//     System.out.println("SelectByGeoID-Time100: "+(endTimeSelectByGeoID - startTimeSelectByGeoID));
//     long startTimeSelectByGeoName = System.currentTimeMillis();
//     PhoenixSQLOperation.selectAndQueryByGeoHashValueLongsToTestPhoenixSecondIndex(geoHashValueLongs);
//     long endTimeSelectByGeoName = System.currentTimeMillis();
//     System.out.println("SelectByGeoName-Time100: "+(endTimeSelectByGeoName - startTimeSelectByGeoName));

        //GeoHash字符串查询算法测试
//        RectangleStrBinaryPrefix rSBP = GeoHashConversion.getStrBinaryRectanglePrefixFromRectangleQueryScope(rQS1);
//     RectangleQueryScope rQSResult = GeoHashConversion.getRectangleQueryScopeFromStrBinaryPrefix(rSBP);
//     System.out.println(rQSResult.toString());
        //System.out.println(rSBP.toString());
//        String strGeoHashBinary = GeoHashConversion.encodeGeoHashFromLonAndLatBinaryString(rQS1.xLongitudeBL,rQS1.yLatitudeBL);
//        System.out.println(strGeoHashBinary);
//     System.out.println(Long.parseLong(strGeoHashBinary,2));
//     double[] xyBL = GeoHashConversion.decodeLonAndLatFromGeoHashBinaryString(strGeoHashBinary);
//     System.out.println(xyBL[0]+","+xyBL[1]);

        //GeoHash叶节点合并测试
        for(int searchManualTestMerge=10;searchManualTestMerge<=20;searchManualTestMerge++){
            System.out.println("================="+"SearchDepthManual: "+searchManualTestMerge+"===============");
            long startTimeGeoHashLongs = System.currentTimeMillis();
            Stack<long[]> gGeoHashLongsMerge =
                    GeoHashConversion.rangeQueryWithGeoHashIndexAccordingToRectangleQueryScoreLeafMerge(r, searchManualTestMerge);
            long endTimeGeoHashLongs = System.currentTimeMillis();
            long startTimeGeoHashLongsMerge = System.currentTimeMillis();
            Stack<long[]> gGeoHashLongsMergeTest =
                    GeoHashConversion.rangeQueryWithGeoHashIndexAccordingToRectangleQueryScoreLeafMergeTest(r,searchManualTestMerge);
            long endTimeGeoHashLongsMerge = System.currentTimeMillis();
            System.out.println("GetGeoHashLongsAndMerge-Time: "+(endTimeGeoHashLongs-startTimeGeoHashLongs)
                    +"#"+(endTimeGeoHashLongsMerge-startTimeGeoHashLongsMerge));
            System.out.println("GetGeoHashLongsAndMerge-Size: "+gGeoHashLongsMerge.size() +"#"+gGeoHashLongsMergeTest.size());
//            for(long[] g:gGeoHashLongsMerge){
//                System.out.println(g[0]+"#"+g[1]);
//            }
//            System.out.println("------------------------------------------");
//            for(long[] gm:gGeoHashLongsMergeTest){
//                System.out.println(gm[0]+"#"+gm[1]);
//            }
//
        }
        PhoenixSQLOperation.closeConnectionWithHBase();


    }
}
