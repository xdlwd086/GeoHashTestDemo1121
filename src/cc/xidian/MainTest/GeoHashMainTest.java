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

/**
 * Created by hadoop on 2016/9/8.
 */
public class GeoHashMainTest {
    public static void main(String[] args) throws Exception{
        PhoenixSQLOperation.getConnectionWithHBase();//建立连接

        //1、通过Phoenix在HBase上创建表并写入1000万条数据，以下操作只执行一次
        //PhoenixSQLOperation.createAndInsertRecordToTableNamedGeoPointTableString();//只执行一次
        //PhoenixSQLOperation.dropTableNamedGeoPointTable();//只执行一次

        //PhoenixSQLOperation.selectResultsToFile();//只执行一次
       PhoenixSQLOperation.dropIndexOfName();//删除二级索引，该操作只执行一次
////
//       long startTimeSecondIndex = System.currentTimeMillis();
//////        PhoenixSQLOperation.createSecondIndexForGeoNameOfTable();
////////        PhoenixSQLOperation.createSecondIndexForGeoPointTableLWD1MLong();
//        //PhoenixSQLOperation.createSecondIndexForGeoHashValueBase32OfTable();
//        PhoenixSQLOperation.createSecondIndexForGeoHashValueLongOfTable();
//        long endTimeSecondIndex = System.currentTimeMillis();
//        System.out.println("CreateSecondIndex-Time: "+(endTimeSecondIndex - startTimeSecondIndex));

        //2、查询操作
//        RectangleQueryScope rQS = new RectangleQueryScope();//构建进行查询范围对象
//        rQS.RectangleQueryScopeDelta(-10.62569,15.50321,22,15);//查询范围初始化
//        //不同矩形查询范围的设计
        RectangleQueryScope rQS1 = new RectangleQueryScope(10.62569,15.50321,32.62569,30.50321);//第一象限中的矩形范围
        RectangleQueryScope rQS2 = new RectangleQueryScope(-62.36512,1.25643,-49.25943,32.65124);//第二象限中的矩形范围
        RectangleQueryScope rQS3 = new RectangleQueryScope(-21.62513,-41.58712,-9.25314,-19.25843);//第三象限中的矩形范围
        RectangleQueryScope rQS4 = new RectangleQueryScope(29.25876,-52.36914,42.25843,-33.58243);//第四象限中的矩形范围
        RectangleQueryScope rQS12 = new RectangleQueryScope(-16.28412,11.58943,9.25846,35.26849);//横跨第一、二象限的矩形范围
        RectangleQueryScope rQS23 = new RectangleQueryScope(-34.25894,-28.33256,-8.25931,4.26985);//横跨第二、三象限的矩形范围
        RectangleQueryScope rQS34 = new RectangleQueryScope(-15.28694,-52.36942,10.25897,-31.25896);//横跨第三、四象限的矩形范围
        RectangleQueryScope rQS41 = new RectangleQueryScope(48.23654,-19.28654,64.25987,13.25897);//横跨第四、一象限的矩形范围
        RectangleQueryScope rQS1234 = new RectangleQueryScope(-14.22314,-25.18972,29.55846,18.22579);//横跨第一二三四象限的矩形范围
        RectangleQueryScope rQS1234min = new RectangleQueryScope(-4.22314,-5.18972,5.55846,4.22579);//横跨第一二三四象限的矩形范围
////        ArrayList<RectangleQueryScope> rQSs = new ArrayList<RectangleQueryScope>();
////        rQSs.add(rQS1);
////        rQSs.add(rQS2);
////        rQSs.add(rQS3);
////        rQSs.add(rQS4);
////        rQSs.add(rQS12);
////        rQSs.add(rQS23);
////        rQSs.add(rQS34);
////        rQSs.add(rQS41);
////        rQSs.add(rQS1234);
//        HashMap<String,RectangleQueryScope> hashMap = new HashMap<String, RectangleQueryScope>();
//        hashMap.put("rQS1",rQS1);
//        hashMap.put("rQS2",rQS2);
//        hashMap.put("rQS3",rQS3);
//        hashMap.put("rQS4",rQS4);
//        hashMap.put("rQS12",rQS12);
//        hashMap.put("rQS23",rQS23);
//        hashMap.put("rQS34",rQS34);
//        hashMap.put("rQS41",rQS41);
//        hashMap.put("rQS1234",rQS1234);
//        //Map.Entry<String,RectangleQueryScope> r = hashMap.get()
        RectangleQueryScope r = new RectangleQueryScope();
        r = rQS1234min;
//        for(Map.Entry<String,RectangleQueryScope> r:hashMap.entrySet()){
            System.out.println("================================"+"rQS1234min"+"=====================================");
            //int searchDepthManual = 1;//搜索深度
            //2.1 无索引的范围查询，遍历所有记录，复杂度为O(n)
//            long startTimeQueryWithoutIndex = System.currentTimeMillis();
//            ArrayList<GeoPointTableRecord> geoPointTableRecordsWithoutIndex
//                    = PhoenixSQLOperation.selectAndQueryRecordsWithoutGeoHashIndex(r);
//            long endTimeQueryWithoutIndex = System.currentTimeMillis();
//            //for(GeoPointTableRecord g:geoPointTableRecordsWithoutIndex){
//            //System.out.println(g.toString());
//            //}
//            System.out.println("RectangleQueryScope: " + r.toString());
//            System.out.println("RectangleRangeQueryWithoutIndex-SizeAndTime: "+geoPointTableRecordsWithoutIndex.size()
//                    +"#"+(endTimeQueryWithoutIndex - startTimeQueryWithoutIndex)/1000.0);
//            System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
            //2.2 SQL+直接查询
            long startTimeQueryWithDirectJudge = System.currentTimeMillis();
            ArrayList<GeoPointTableRecord> gPTRWithDirectJudge = PhoenixSQLOperation.selectAndQueryRecordsWithDirectJudge(r);
            long endTimeQueryWithDirectJudge = System.currentTimeMillis();
            System.out.println("RectangleQueryScope: "+r.toString());
            System.out.println("RectangleRangeQueryWithIndex-SizeAndTime:"+gPTRWithDirectJudge.size()
                    +"#"+(endTimeQueryWithDirectJudge - startTimeQueryWithDirectJudge)/1000.0);
            System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
////            //2.3 SQL+UDF函数查询
//            long startTimeQueryWithUDFFunction = System.currentTimeMillis();
//            ArrayList<GeoPointTableRecord> gPTRWithUDFFunction = PhoenixSQLOperation.selectAndQueryRecordsWithUDFFunction(r);
//            long endTimeQueryWithUDFFunction = System.currentTimeMillis();
//            System.out.println("RectangleQueryScope: "+r.toString());
//            System.out.println("RectangleRangeQueryWithIndex-SizeAndTime:"+gPTRWithUDFFunction.size()
//                    +"#"+(endTimeQueryWithUDFFunction - startTimeQueryWithUDFFunction)/1000.0);
            //2.4GeoHash查询，三种情况，改变搜索深度，进行查询
            for(int searchDepthManual = 5;searchDepthManual<=20;searchDepthManual++) {
                System.out.println("-----------------------------------------------------------------");
                long startTimeQueryWithGeoHashAndSecondFiltering = System.currentTimeMillis();
                ArrayList<GeoPointTableRecord> gPTRWithGeoHashAndSecondFiltering =
                        PhoenixSQLOperation.selectAndQueryRecordsWithLongGeoHashIndexAndSecondFiltering(r, searchDepthManual);
                long endTimeQueryWithGeoHashAndSecondFiltering = System.currentTimeMillis();
                ArrayList<long[]> gGeoHashLongs =
                        GeoHashConversion.rangeQueryWithGeoHashIndexAccordingToRectangleQueryScore(r, searchDepthManual);
//                ArrayList<String[]> gGeoHashLongs =
//                        GeoHashConversion.rangeQueryWithGeoHashIndexAccordingToRectangleQueryScore60(r,searchDepthManual);
                System.out.println("RectangleQueryScope: " + r.toString());
                System.out.println("GeoHashIndexAlgorithm-SearchDepthAndSearchTime: " + searchDepthManual
                        + "#" + (endTimeQueryWithGeoHashAndSecondFiltering - startTimeQueryWithGeoHashAndSecondFiltering) / 1000.0);
                System.out.println("GeoHashLongsAndRectangleRangeQueryWithIndex-Size: "
                        + gGeoHashLongs.size() + "#" + gPTRWithGeoHashAndSecondFiltering.size());
            }


//                long startTimeQueryWithGeoHashAndSecondFiltering = System.currentTimeMillis();
//                ArrayList<GeoPointTableRecord> gPTRWithGeoHashAndSecondFiltering =
//                        PhoenixSQLOperation.selectAndQueryRecordsWithGeoHashIndexAndSecondFiltering(r, searchDepthManual);
//                long endTimeQueryWithGeoHashAndSecondFiltering = System.currentTimeMillis();
//                long startTimeQueryWithGeoHashAndDirectJudge = System.currentTimeMillis();
//                ArrayList<GeoPointTableRecord> gPTRWithGeoHashAndDirectJudge =
//                        PhoenixSQLOperation.selectAndQueryRecordsWithGeoHashIndexAndDirectJudge(r, searchDepthManual);
//                long endTimeQueryWithGeoHashAndDirectJudge = System.currentTimeMillis();
//                long startTimeQueryWithGeoHashAndUDFFunction = System.currentTimeMillis();
//                ArrayList<GeoPointTableRecord> gPTRWithGeoHashAndUDFFunction =
//                        PhoenixSQLOperation.selectAndQueryRecordsWithGeoHashIndexAndUDFFunction(r,searchDepthManual);
//                long endTimeQueryWithGeoHashAndUDFFunction = System.currentTimeMillis();
//                //for(GeoPointTableRecord g:geoPointTableRecordsWithIndexTest){
//                //System.out.println(g.toString());
//                //}
//                ArrayList<long[]> gGeoHashLongs =
//                        GeoHashConversion.rangeQueryWithGeoHashIndexAccordingToRectangleQueryScore(r,searchDepthManual);
//                System.out.println("RectangleQueryScope: " + r.toString());
//                System.out.println("GeoHashIndexAlgorithm-SearchDepthAndSearchTime: "+searchDepthManual
//                        +"#"+(endTimeQueryWithGeoHashAndSecondFiltering - startTimeQueryWithGeoHashAndSecondFiltering)/1000.0
//                        +"%"+(endTimeQueryWithGeoHashAndDirectJudge-startTimeQueryWithGeoHashAndDirectJudge)/1000.0
//                        +"%"+(endTimeQueryWithGeoHashAndUDFFunction-startTimeQueryWithGeoHashAndUDFFunction)/1000.0);
//                System.out.println("GeoHashLongsAndRectangleRangeQueryWithIndex-Size: "
//                        + gGeoHashLongs.size()+"#"+gPTRWithGeoHashAndSecondFiltering.size()
//                        +"%"+gPTRWithGeoHashAndDirectJudge.size()+"%"+gPTRWithGeoHashAndUDFFunction.size());
//                String strSDTGeoHashAndSecondFiltering = searchDepthManual
//                        +","+((endTimeQueryWithGeoHashAndSecondFiltering-startTimeQueryWithGeoHashAndSecondFiltering)/1000.0)+"\n";
//                File fileSDTGeoHashAndSecondFiltering = new File("rQS2SDTGeoHashAndSecondFiltering.txt");
//                FileUtil.writeToFile(fileSDTGeoHashAndSecondFiltering,strSDTGeoHashAndSecondFiltering);
//                String strSDTGeoHashAndDirectJudge = searchDepthManual
//                        +","+((endTimeQueryWithGeoHashAndDirectJudge-startTimeQueryWithGeoHashAndDirectJudge)/1000.0)+"\n";
//                File fileSDTGeoHashAndDirectJudge = new File("rQS2SDTGeoHashAndDirectJudge.txt");
//                FileUtil.writeToFile(fileSDTGeoHashAndDirectJudge,strSDTGeoHashAndDirectJudge);
//                String strSDTGeoHashAndUDFFunction = searchDepthManual
//                        +","+((endTimeQueryWithGeoHashAndUDFFunction-startTimeQueryWithGeoHashAndUDFFunction)/1000.0)+"\n";
//                File fileSDTGeoHashAndUDFFunction = new File("rQS2SDTGeoHashAndUDFFunction.txt");
//                FileUtil.writeToFile(fileSDTGeoHashAndUDFFunction,strSDTGeoHashAndUDFFunction);

           //}
        //}

//        long startTimeQueryWithIndexTest = System.currentTimeMillis();
//        ArrayList<GeoPointTableRecord> geoPointTableRecordsWithIndexTest =
//                              PhoenixSQLOperation.selectAndQueryRecordsWithGeoHashIndexZY(rQS41,searchDepthManual);
//        long endTimeQueryWithIndexTest = System.currentTimeMillis();
//        for(GeoPointTableRecord g:geoPointTableRecordsWithIndexTest){
//            System.out.println(g.toString());
//        }
//        ArrayList<long[]> gGeoHashLongs = GeoHashConversion.rangeQueryWithGeoHashIndexAccordingToRectangleQueryScore(rQS41,searchDepthManual);
//        System.out.println("RectangleQueryScope: "+rQS41.toString());
//        System.out.println("GeoHashIndexAlgorithm-SearchDepth: "+searchDepthManual);
//        System.out.println("GeoHashLongs-Size:"+gGeoHashLongs.size());
//        System.out.println("RectangleRangeQueryWithIndex-Size: " + geoPointTableRecordsWithIndexTest.size());
//        System.out.println("RectangleRangeQueryWithIndex-Time: "+(endTimeQueryWithIndexTest - startTimeQueryWithIndexTest));

//        ArrayList<long[]> gGeoHashLongs = GeoHashConversion.rangeQueryWithGeoHashIndexAccordingToRectangleQueryScore(ra);
//        System.out.println(gGeoHashLongs.size());
//        RectanglePrefix rP = GeoHashConversion.getRectanglePrefixFromRectangleQueryScope(ra);
//        System.out.println(rP.toString());
//        RectangleQueryScope rQSResult = GeoHashConversion.getRectangleQueryScopeFromPrefix(rP);
//        System.out.println(rQSResult.toString());
//        System.out.println(ra.toString());

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


        PhoenixSQLOperation.closeConnectionWithHBase();


    }
}
