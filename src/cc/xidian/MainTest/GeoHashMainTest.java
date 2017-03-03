package cc.xidian.MainTest;

import cc.xidian.GeoHash.GeoHashConversion;
import cc.xidian.GeoHash.RectanglePrefix;
//import cc.xidian.GeoHash.RectangleStrBinaryPrefix;
import cc.xidian.GeoObject.*;
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
//        PhoenixSQLOperation.getConnectionWithHBase();//建立连接

        PhoenixSQLOperation.getConnectionWithHBaseXenServerHDP();

        //2、查询操作
        //2.1 19种矩形查询范围的设计
        RectangleQueryScope rQS0_1 = new RectangleQueryScope(10.62569,15.50321,32.62569,30.50321);//第一象限中的矩形范围
        RectangleQueryScope rQS1_1min = new RectangleQueryScope(10.62569,16.50321,11.25984,17.36458);//第一象限中的矩形范围
        RectangleQueryScope rQS2_2 = new RectangleQueryScope(-62.36512,1.25643,-49.25943,32.65124);//第二象限中的矩形范围
        RectangleQueryScope rQS3_2min = new RectangleQueryScope(-62.36512,1.25643,-61.25943,2.65124);//第二象限中的矩形范围
        RectangleQueryScope rQS4_3 = new RectangleQueryScope(-21.62513,-41.58712,-9.25314,-19.25843);//第三象限中的矩形范围
        RectangleQueryScope rQS5_3min = new RectangleQueryScope(-21.62513,-41.58712,-12.25314,-29.25843);//第三象限中的矩形范围
        RectangleQueryScope rQS6_4 = new RectangleQueryScope(29.25876,-52.36914,42.25843,-33.58243);//第四象限中的矩形范围
        RectangleQueryScope rQS7_4min = new RectangleQueryScope(29.25876,-52.36914,30.25843,-50.58243);//第四象限中的矩形范围
        RectangleQueryScope rQS8_12 = new RectangleQueryScope(-16.28412,11.58943,9.25846,35.26849);//横跨第一、二象限的矩形范围
        RectangleQueryScope rQS9_12min = new RectangleQueryScope(-1.28412,11.98943,0.25846,12.26849);//横跨第一、二象限的矩形范围
        RectangleQueryScope rQS10_23 = new RectangleQueryScope(-34.25894,-28.33256,-8.25931,4.26985);//横跨第二、三象限的矩形范围
        RectangleQueryScope rQS11_23min = new RectangleQueryScope(-34.25894,-1.33256,-33.25931,0.26985);//横跨第二、三象限的矩形范围
        RectangleQueryScope rQS12_34 = new RectangleQueryScope(-15.28694,-52.36942,10.25897,-31.25896);//横跨第三、四象限的矩形范围
        RectangleQueryScope rQS13_34min = new RectangleQueryScope(-3.28694,-52.36942,2.25897,-51.25896);//横跨第三、四象限的矩形范围
        RectangleQueryScope rQS14_41 = new RectangleQueryScope(48.23654,-19.28654,64.25987,13.25897);//横跨第四、一象限的矩形范围
        RectangleQueryScope rQS15_41min = new RectangleQueryScope(48.23654,-1.28654,49.25987,2.25897);//横跨第四、一象限的矩形范围
        RectangleQueryScope rQS16_1234 = new RectangleQueryScope(-14.22314,-25.18972,29.55846,18.22579);//横跨第一二三四象限的矩形范围
        RectangleQueryScope rQS17_1234min = new RectangleQueryScope(-4.22314,-5.18972,5.55846,4.22579);//横跨第一二三四象限的矩形范围
        RectangleQueryScope rQS18_1234min2 = new RectangleQueryScope(-1.02314,-1.18972,1.05846,1.02579);//横跨第一二三四象限的矩形范围
        //创建通用的查询范围对象
        RectangleQueryScope r = new RectangleQueryScope();
        r = rQS18_1234min2;

        //GeoHash新查询算法测试，时间：2016年12月21日09:20:14
        Stack<GeoHashIndexRecord> geoHashIndexRecordStack = GeoHashConversion.getMergedGeoHashLongsByGeoHashIndexAlgorithmWithBFSAndLevelMaxTest(r);
        GeoHashIndexRecord g = geoHashIndexRecordStack.pop();
//        for(GeoHashIndexRecord g:geoHashIndexRecordStack) {
//            System.out.println(g.toString());

//            GeoHashIndexRecord g1 = geoHashIndexRecordStack.get(3);
//            for (long[] longs : g.sGeoHashLongs) {
////                System.out.println(longs[0] + " " + longs[1]);
////                System.out.println(Long.toBinaryString(longs[0]));
////                System.out.println(Long.toBinaryString(longs[1]));
//                double[] xyBLTemp = GeoHashConversion.HashToLongLat(longs[0]);
//                double[] xyTRTemp = GeoHashConversion.HashToLongLat(longs[1]);
//                System.out.println(xyBLTemp[0] + "," + xyBLTemp[1] + "," + xyTRTemp[0] + "," + xyTRTemp[1]);
//            }
        for (int i=0;i<100;i++){
            System.out.println("==================================");
            for(RectanglePrefix rP:g.rPArray){
                long[] geoHashLongs = GeoHashConversion.getGeoHashLongsFromPrefix(rP);
//                System.out.println(geoHashLongs[0]+"#"+geoHashLongs[1]);
                RectangleQueryScope rQSQueue = GeoHashConversion.getRectangleQueryScopeFromPrefix(rP);
//                System.out.println(rQSQueue.toString());
            }

                long startTimeQueryWithGeoHashAndSecondFiltering10M = System.currentTimeMillis();
                ArrayList<GeoPointTableRecordSimple> gPTRWithGeoHashAndSecondFiltering10M =
                        PhoenixSQLOperation.selectAndQueryRecordsWithGeoHashIndexAreaRatioAndSecondFilteringUnionAllFrom10MTableTest(g.sGeoHashLongs, r);
                long endTimeQueryWithGeoHashAndSecondFiltering10M = System.currentTimeMillis();
                //2.4.2 SQL-GeoHash的BetweenAnd与直接判断的UnionAll
                long startTimeQueryWithGeoHashAndDirectJudge10M = System.currentTimeMillis();
                ArrayList<GeoPointTableRecordSimple> gPTRWithGeoHashAndDirectJudge10M =
                        PhoenixSQLOperation.selectAndQueryRecordsWithGeoHashIndexAreaRatioAndDirectJudgeUnionAllFrom10MTableTest(g.sGeoHashLongs, r);
                long endTimeQueryWithGeoHashAndDirectJudge10M = System.currentTimeMillis();
                //2.4.3 SQL-GeoHash的BetweenAnd与UDF函数的UnionAll
                long startTimeQueryWithGeoHashAndUDFFunction10M = System.currentTimeMillis();
                ArrayList<GeoPointTableRecordSimple> gPTRWithGeoHashAndUDFFunction10M =
                        PhoenixSQLOperation.selectAndQueryRecordsWithGeoHashIndexAreaRatioAndUDFFunctionUnionAllFrom10MTableTest(g.sGeoHashLongs, r);
                long endTimeQueryWithGeoHashAndUDFFunction10M = System.currentTimeMillis();

                System.out.println("RectangleQueryScope: " + r.toString());
                System.out.println("GeoHashIndexAlgorithm-AreaAndSearchTime: " + g.searchDepth
                        + "#" + (endTimeQueryWithGeoHashAndSecondFiltering10M - startTimeQueryWithGeoHashAndSecondFiltering10M) / 1000.0
                        + "%" + (endTimeQueryWithGeoHashAndDirectJudge10M - startTimeQueryWithGeoHashAndDirectJudge10M) / 1000.0
                        + "%" + (endTimeQueryWithGeoHashAndUDFFunction10M - startTimeQueryWithGeoHashAndUDFFunction10M) / 1000.0);
                System.out.println("GeoHashLongsAndRectangleRangeQueryWithIndex-Size: "
                        + g.sizeOfGeoHashLongs + "#" + gPTRWithGeoHashAndSecondFiltering10M.size()
                        + "%" + gPTRWithGeoHashAndDirectJudge10M.size() + "%" + gPTRWithGeoHashAndUDFFunction10M.size());
                System.out.println(g.toString());
                String strSDTGeoHashThreeAll = g.searchDepth + " "
                        + ((endTimeQueryWithGeoHashAndSecondFiltering10M - startTimeQueryWithGeoHashAndSecondFiltering10M) / 1000.0 + " ")
                        + ((endTimeQueryWithGeoHashAndDirectJudge10M - startTimeQueryWithGeoHashAndDirectJudge10M) / 1000.0 + " ")
                        + ((endTimeQueryWithGeoHashAndUDFFunction10M - startTimeQueryWithGeoHashAndUDFFunction10M) / 1000.0) + "\n";
//                File fileSDTGeoHashThreeAll = new File("rQS1_1minSDTGeoHashSDU100SumMergeAreaRatio100MNew08.txt");
//                FileUtil.writeToFile(fileSDTGeoHashThreeAll, strSDTGeoHashThreeAll);

        }
//        System.out.println(g.toString());
//        long startSelectTest = System.currentTimeMillis();
//        ArrayList<GeoPointTableRecordSimple> geoPointTableRecordSimples = PhoenixSQLOperation.SelectTest(r);
//        long endSelectTest = System.currentTimeMillis();
//        System.out.println(geoPointTableRecordSimples.size());
//        System.out.println("SelectTest-Time: "+(endSelectTest - startSelectTest));

//        GeoHashConversion.getMergedGeoHashLongsByGeoHashIndexAlgorithmWithCenterLWD(r);




//        double xLongitudeBLFly = -160.23541;
//        double yLatitudeBLFly = -85.25489;
//        double deltaXFly = 3;
//        double deltaYFly = 2.5;
//        RectangleQueryScope rQSFly = new RectangleQueryScope();
//        rQSFly = rQS1_1min;
////        rQSFly.RectangleQueryScopeDelta(xLongitudeBLFly,yLatitudeBLFly,deltaXFly,deltaYFly);
//        int count1 = 0;
//        for(int a=0;a<100;a++){
////            xLongitudeBLFly+=16;
////            double yLatitudeBLFly = -85.25489;
////            for(int b=0;b<15;b++){
////                yLatitudeBLFly+=10;
////                RectangleQueryScope rQSFly = new RectangleQueryScope();
////                rQSFly.RectangleQueryScopeDelta(xLongitudeBLFly,yLatitudeBLFly,deltaXFly,deltaYFly);
//
//                int count =0;
//                for (double areaRatioTemp = 1.1; areaRatioTemp <= 4.1; areaRatioTemp+=0.1) {
//                    count++;
//                    DecimalFormat df = new DecimalFormat("#.0000");
//                    double areaRatio = Double.parseDouble(df.format(areaRatioTemp));
////                    System.out.println(areaRatio);
//                    System.out.println("---------------------------"+count1+"--------------------------------------");
//                    long startTimeQueryWithGeoHashAndSecondFiltering10M = System.currentTimeMillis();
//                    ArrayList<GeoPointTableRecordSimple> gPTRWithGeoHashAndSecondFiltering10M =
//                            PhoenixSQLOperation.selectAndQueryRecordsWithGeoHashIndexAreaRatioAndSecondFilteringUnionAllFrom10MTable(rQSFly, areaRatio);
//                    long endTimeQueryWithGeoHashAndSecondFiltering10M = System.currentTimeMillis();
//                    //2.4.2 SQL-GeoHash的BetweenAnd与直接判断的UnionAll
//                    long startTimeQueryWithGeoHashAndDirectJudge10M = System.currentTimeMillis();
//                    ArrayList<GeoPointTableRecordSimple> gPTRWithGeoHashAndDirectJudge10M =
//                            PhoenixSQLOperation.selectAndQueryRecordsWithGeoHashIndexAreaRatioAndDirectJudgeUnionAllFrom10MTable(rQSFly, areaRatio);
//                    long endTimeQueryWithGeoHashAndDirectJudge10M = System.currentTimeMillis();
//                    //2.4.3 SQL-GeoHash的BetweenAnd与UDF函数的UnionAll
//                    long startTimeQueryWithGeoHashAndUDFFunction10M = System.currentTimeMillis();
//                    ArrayList<GeoPointTableRecordSimple> gPTRWithGeoHashAndUDFFunction10M =
//                            PhoenixSQLOperation.selectAndQueryRecordsWithGeoHashIndexAreaRatioAndUDFFunctionUnionAllFrom10MTable(rQSFly, areaRatio);
//                    long endTimeQueryWithGeoHashAndUDFFunction10M = System.currentTimeMillis();
//                    //获得GeoHash段集合并计算个数，用于后面打印显示
//                    Stack<long[]> gGeoHashLongs =
//                            GeoHashConversion.getMergedGeoHashLongsByGeoHashIndexAlgorithmWithBFSAndAreaRatio(rQSFly, areaRatio);
//                    //for()
//                    //相关结果输出
//                    System.out.println("RectangleQueryScope: " + rQSFly.toString());
//                    System.out.println("GeoHashIndexAlgorithm-AreaAndSearchTime: " + count
//                            + "#" + (endTimeQueryWithGeoHashAndSecondFiltering10M - startTimeQueryWithGeoHashAndSecondFiltering10M) / 1000.0
//                            + "%" + (endTimeQueryWithGeoHashAndDirectJudge10M - startTimeQueryWithGeoHashAndDirectJudge10M) / 1000.0
//                            + "%" + (endTimeQueryWithGeoHashAndUDFFunction10M - startTimeQueryWithGeoHashAndUDFFunction10M) / 1000.0);
//                    //+ "%" + (endTimeQueryWithGeoHashAndUDFFunctionNew - startTimeQueryWithGeoHashAndUDFFunctionNew) / 1000.0);
//                    System.out.println("AreaRatioAndGeoHashLongsAndRectangleRangeQueryWithIndex-Size: "+areaRatio+"#"
//                            + gGeoHashLongs.size() + "#" + gPTRWithGeoHashAndSecondFiltering10M.size()
//                            + "%" + gPTRWithGeoHashAndDirectJudge10M.size() + "%" + gPTRWithGeoHashAndUDFFunction10M.size());
////////                //相关结果写入文件操作，便于MatLab画图
//                    String strSDTGeoHashThreeAll = count + " "
//                            + ((endTimeQueryWithGeoHashAndSecondFiltering10M - startTimeQueryWithGeoHashAndSecondFiltering10M) / 1000.0 + " ")
//                            + ((endTimeQueryWithGeoHashAndDirectJudge10M - startTimeQueryWithGeoHashAndDirectJudge10M) / 1000.0 + " ")
//                            + ((endTimeQueryWithGeoHashAndUDFFunction10M - startTimeQueryWithGeoHashAndUDFFunction10M) / 1000.0) + "\n";
//                    File fileSDTGeoHashThreeAll = new File("rQSFlySDTGeoHashSDU100SumMergeAreaRatio100M02.txt");
//                    FileUtil.writeToFile(fileSDTGeoHashThreeAll, strSDTGeoHashThreeAll);
//                }
////                System.out.println(count1+","+rQSFly.toString());
//                count1++;
////            }
//        }
//        double xLongitudeBLFly = -160.23541;
//        //double yLatitudeBLFly = -85.25489;
//        double deltaXFly = 3;
//        double deltaYFly = 2.5;
////        RectangleQueryScope rQSFly = new RectangleQueryScope();
////        rQSFly.RectangleQueryScopeDelta(xLongitudeBLFly,yLatitudeBLFly,deltaXFly,deltaYFly);
//        int count1 = 0;
//        for(int a=0;a<20;a++){
//            xLongitudeBLFly+=16;
//            double yLatitudeBLFly = -85.25489;
//            for(int b=0;b<15;b++){
//                yLatitudeBLFly+=10;
//                RectangleQueryScope rQSFly = new RectangleQueryScope();
//                rQSFly.RectangleQueryScopeDelta(xLongitudeBLFly,yLatitudeBLFly,deltaXFly,deltaYFly);
//
//                int count =0;
//                for (double areaRatioTemp = 1.1; areaRatioTemp <= 4.0; areaRatioTemp+=0.1) {
//                    count++;
//                    DecimalFormat df = new DecimalFormat("#.0000");
//                    double areaRatio = Double.parseDouble(df.format(areaRatioTemp));
//                    System.out.println("---------------------------"+count1+"--------------------------------------");
//                    long startTimeQueryWithGeoHashAndSecondFiltering10M = System.currentTimeMillis();
//                    ArrayList<GeoPointTableRecordSimple> gPTRWithGeoHashAndSecondFiltering10M =
//                            PhoenixSQLOperation.selectAndQueryRecordsWithGeoHashIndexAreaRatioAndSecondFilteringUnionAllFrom10MTable(rQSFly, areaRatio);
//                    long endTimeQueryWithGeoHashAndSecondFiltering10M = System.currentTimeMillis();
//                    //2.4.2 SQL-GeoHash的BetweenAnd与直接判断的UnionAll
//                    long startTimeQueryWithGeoHashAndDirectJudge10M = System.currentTimeMillis();
//                    ArrayList<GeoPointTableRecordSimple> gPTRWithGeoHashAndDirectJudge10M =
//                            PhoenixSQLOperation.selectAndQueryRecordsWithGeoHashIndexAreaRatioAndDirectJudgeUnionAllFrom10MTable(rQSFly, areaRatio);
//                    long endTimeQueryWithGeoHashAndDirectJudge10M = System.currentTimeMillis();
//                    //2.4.3 SQL-GeoHash的BetweenAnd与UDF函数的UnionAll
//                    long startTimeQueryWithGeoHashAndUDFFunction10M = System.currentTimeMillis();
//                    ArrayList<GeoPointTableRecordSimple> gPTRWithGeoHashAndUDFFunction10M =
//                        PhoenixSQLOperation.selectAndQueryRecordsWithGeoHashIndexAreaRatioAndUDFFunctionUnionAllFrom10MTable(rQSFly, areaRatio);
//                    long endTimeQueryWithGeoHashAndUDFFunction10M = System.currentTimeMillis();
//                    //获得GeoHash段集合并计算个数，用于后面打印显示
//                    Stack<long[]> gGeoHashLongs =
//                            GeoHashConversion.getMergedGeoHashLongsByGeoHashIndexAlgorithmWithBFSAndAreaRatio(rQSFly, areaRatio);
//                    //for()
//                    //相关结果输出
//                    System.out.println("RectangleQueryScope: " + rQSFly.toString());
//                    System.out.println("GeoHashIndexAlgorithm-AreaAndSearchTime: " + count
//                            + "#" + (endTimeQueryWithGeoHashAndSecondFiltering10M - startTimeQueryWithGeoHashAndSecondFiltering10M) / 1000.0
//                            + "%" + (endTimeQueryWithGeoHashAndDirectJudge10M - startTimeQueryWithGeoHashAndDirectJudge10M) / 1000.0
//                            + "%" + (endTimeQueryWithGeoHashAndUDFFunction10M - startTimeQueryWithGeoHashAndUDFFunction10M) / 1000.0);
//                    //+ "%" + (endTimeQueryWithGeoHashAndUDFFunctionNew - startTimeQueryWithGeoHashAndUDFFunctionNew) / 1000.0);
//                    System.out.println("GeoHashLongsAndRectangleRangeQueryWithIndex-Size: "
//                            + gGeoHashLongs.size() + "#" + gPTRWithGeoHashAndSecondFiltering10M.size()
//                            + "%" + gPTRWithGeoHashAndDirectJudge10M.size() + "%" + gPTRWithGeoHashAndUDFFunction10M.size());
////////                //相关结果写入文件操作，便于MatLab画图
//                String strSDTGeoHashThreeAll = count + " "
//                        + ((endTimeQueryWithGeoHashAndSecondFiltering10M - startTimeQueryWithGeoHashAndSecondFiltering10M) / 1000.0 + " ")
//                        + ((endTimeQueryWithGeoHashAndDirectJudge10M - startTimeQueryWithGeoHashAndDirectJudge10M) / 1000.0 + " ")
//                        + ((endTimeQueryWithGeoHashAndUDFFunction10M - startTimeQueryWithGeoHashAndUDFFunction10M) / 1000.0) + "\n";
//                File fileSDTGeoHashThreeAll = new File("rQSFlySDTGeoHashSDU300SumMergeAreaRatio30M28.txt");
//                FileUtil.writeToFile(fileSDTGeoHashThreeAll, strSDTGeoHashThreeAll);
//                }
//                System.out.println(count1+","+rQSFly.toString());
//                count1++;
//            }
//        }


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
//        for(int i=0;i<100;i++) {
//            System.out.println("================================" + i+ "=====================================");
//            long startTimeTest = System.currentTimeMillis();
//            Stack<GeoHashIndexRecord> gHIRArray = GeoHashConversion.getMergedGeoHashLongsByGeoHashIndexAlgorithmWithBFSAndAreaRatioLWDTest(r);
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
//                System.out.println(g.toString());
//                String strSDTGeoHashThreeAll = g.searchDepth + " "
//                        + ((endTimeQueryWithGeoHashAndSecondFiltering10M - startTimeQueryWithGeoHashAndSecondFiltering10M) / 1000.0 + " ")
//                        + ((endTimeQueryWithGeoHashAndDirectJudge10M - startTimeQueryWithGeoHashAndDirectJudge10M) / 1000.0 + " ")
//                        + ((endTimeQueryWithGeoHashAndUDFFunction10M - startTimeQueryWithGeoHashAndUDFFunction10M) / 1000.0) + "\n";
////                File fileSDTGeoHashThreeAll = new File("rQS1_1minSDTGeoHashSDU100SumMergeAreaRatio100MNew08.txt");
////                FileUtil.writeToFile(fileSDTGeoHashThreeAll, strSDTGeoHashThreeAll);
//            }
//
//            System.out.println("Size: " + gHIRArray.size());
//            System.out.println("Time: " +(endTimeTest - startTimeTest));
//        }

        //时空查询GDELT数据集
        RectangleQueryScopeWithTime rQST0 = new RectangleQueryScopeWithTime(115.25,39.26,117.30,41.03,20160501,20160507);
        RectangleQueryScopeWithTime rQST1 = new RectangleQueryScopeWithTime(115.25,39.26,117.30,41.03,20161001,20161007);
        RectangleQueryScopeWithTime rQST2 = new RectangleQueryScopeWithTime(-75.213,39.14,-73.228,41.25,20161101,20161108);
        RectangleQueryScopeWithTime rQST3 = new RectangleQueryScopeWithTime(34.114,29.542,36.942,32.429,20160214,20160301);
        RectangleQueryScopeWithTime rQST4 = new RectangleQueryScopeWithTime(-45.127,-23.281,-41.558,-21.256,20160805,20160821);
        RectangleQueryScopeWithTime rQST5 = new RectangleQueryScopeWithTime(115.25,39.26,117.30,41.03,20161202,20161220);
//        RectangleQueryScopeWithTime rt = rQST5;
//        double areaRatio = 1.5;
//
//        long sumOfGetHashs = 0;
//        long sumOfGDELTQuery = 0;
//        int sizeOfGetHashs = 0;
//        int sizeOfGDELTQuery = 0;
//        for(int i=0;i<100;i++){
//            long startTimeGetGeoHashs = System.currentTimeMillis();
//            Stack<long[]> geoHashLongQueryResults = GeoHashConversion.getMergedGeoHashLongsByGeoHashIndexAlgorithmWithBFSAndAreaRatio(rt.rQS,areaRatio);
//            long endTimeGetGeoHashs = System.currentTimeMillis();
//            long startTimeGDELTQuery = System.currentTimeMillis();
//            ArrayList<GDELTEventRecordSimpleA> gdeltEventRecordSimpleAs =
//                    PhoenixSQLOperation.selectAndQueryGDELTEventRecordsWithGeoHashIndexAreaRatioAndUDFFunctionAndDateConstraintUnionAll(geoHashLongQueryResults,rt);
//            long endTimeGDELTQuery = System.currentTimeMillis();
////            for(GDELTEventRecordSimpleA gd:gdeltEventRecordSimpleAs){
////                System.out.println(gd.toString());
////            }
//            sumOfGetHashs += endTimeGetGeoHashs - startTimeGetGeoHashs;
//            sumOfGDELTQuery += endTimeGDELTQuery - startTimeGDELTQuery;
//            sizeOfGetHashs = geoHashLongQueryResults.size();
//            sizeOfGDELTQuery = gdeltEventRecordSimpleAs.size();
//            System.out.println("=============================="+i+"================================");
//            System.out.println("getGeoHashs-SizeAndTime: " + sizeOfGetHashs+"#"+(endTimeGetGeoHashs - startTimeGetGeoHashs)/1000.0);
//            System.out.println("GDELTQuery-SizeAndTime: "+sizeOfGDELTQuery+"#"+(endTimeGDELTQuery - startTimeGDELTQuery)/1000.0);
//        }
//        System.out.println("getGeoHashs-SizeAndAverageTime: "+ sizeOfGetHashs+"#"+sumOfGetHashs/100000.0);
//        System.out.println("GDELTQuery-SizeAndAverageTime: "+ sizeOfGDELTQuery+"#"+sumOfGDELTQuery/100000.0);


//        File fileGDELTEventXY = new File("GDLETEvent2016XYA.txt");
//        PhoenixSQLOperation.getXYFromGDELTEvent2016ToFile(fileGDELTEventXY);

        PhoenixSQLOperation.closeConnectionWithHBase();


    }
}
