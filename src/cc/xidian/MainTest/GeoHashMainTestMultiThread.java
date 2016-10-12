package cc.xidian.MainTest;

import cc.xidian.GeoHash.GeoHashConversion;
import cc.xidian.GeoObject.GeoPointTableRecord;
import cc.xidian.GeoObject.RectangleQueryScope;
import cc.xidian.PhoenixOperation.PhoenixSQLOperation;
import cc.xidian.geoUtil.FileUtil;

import java.io.File;
import java.util.ArrayList;

/**
 * Created by hadoop on 2016/9/25.
 */
public class GeoHashMainTestMultiThread implements Runnable{
    private String nameThread;
    public GeoHashMainTestMultiThread(){
        this.nameThread = "";
    }
    public GeoHashMainTestMultiThread(String nameThread){
        this.nameThread = nameThread;
    }
    public void run(){
        PhoenixSQLOperation.getConnectionWithHBase();
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
        r = rQS5_3min;
        System.out.println("================================"+"rQS5_3min"+"=====================================");
//        long startTimeQueryWithDirectJudge = System.currentTimeMillis();
//        ArrayList<GeoPointTableRecord> gPTRWithDirectJudge = PhoenixSQLOperation.selectAndQueryRecordsWithDirectJudge(r);
//        long endTimeQueryWithDirectJudge = System.currentTimeMillis();
//        System.out.println("RectangleQueryScope: "+r.toString());
//        System.out.println("RectangleRangeQueryWithIndex-SizeAndTime:"+gPTRWithDirectJudge.size()
//                +"#"+(endTimeQueryWithDirectJudge - startTimeQueryWithDirectJudge)/1000.0);
//        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");

        //2.4GeoHash查询，三种情况，改变搜索深度，进行查询
//        for(int searchDepthManual = 1;searchDepthManual<=20;searchDepthManual++) {
//            //System.out.println("-----------------------------------------------------------------");
//            //2.4.1 SQL-GeoHash的BetweenAnd的UnionAll+本地内存二次过滤
//            long startTimeQueryWithGeoHashAndSecondFiltering = System.currentTimeMillis();
//            ArrayList<GeoPointTableRecord> gPTRWithGeoHashAndSecondFiltering =
//                    PhoenixSQLOperation.selectAndQueryRecordsWithGeoHashIndexAndSecondFilteringUnionAll(r, searchDepthManual);
//            long endTimeQueryWithGeoHashAndSecondFiltering = System.currentTimeMillis();
//            //2.4.2 SQL-GeoHash的BetweenAnd与直接判断的UnionAll
//            long startTimeQueryWithGeoHashAndDirectJudge = System.currentTimeMillis();
//            ArrayList<GeoPointTableRecord> gPTRWithGeoHashAndDirectJudge =
//                    PhoenixSQLOperation.selectAndQueryRecordsWithGeoHashIndexAndDirectJudgeUnionAll(r, searchDepthManual);
//            long endTimeQueryWithGeoHashAndDirectJudge = System.currentTimeMillis();
//            //2.4.3 SQL-GeoHash的BetweenAnd与UDF函数的UnionAll
//            long startTimeQueryWithGeoHashAndUDFFunction = System.currentTimeMillis();
//            ArrayList<GeoPointTableRecord> gPTRWithGeoHashAndUDFFunction =
//                    PhoenixSQLOperation.selectAndQueryRecordsWithGeoHashIndexAndUDFFunctionUnionAll(r, searchDepthManual);
//            long endTimeQueryWithGeoHashAndUDFFunction = System.currentTimeMillis();
//            //获得GeoHash段集合并计算个数，用于后面打印显示
//            ArrayList<long[]> gGeoHashLongs =
//                    GeoHashConversion.rangeQueryWithGeoHashIndexAccordingToRectangleQueryScore(r, searchDepthManual);
//            //相关结果输出
//            System.out.println("RectangleQueryScope: " + r.toString());
//            System.out.println("GeoHashIndexAlgorithm-SearchDepthAndSearchTime: "+searchDepthManual
//                    +"#"+(endTimeQueryWithGeoHashAndSecondFiltering - startTimeQueryWithGeoHashAndSecondFiltering)/1000.0
//                    +"%"+(endTimeQueryWithGeoHashAndDirectJudge-startTimeQueryWithGeoHashAndDirectJudge)/1000.0
//                    +"%"+(endTimeQueryWithGeoHashAndUDFFunction-startTimeQueryWithGeoHashAndUDFFunction)/1000.0);
//            System.out.println("GeoHashLongsAndRectangleRangeQueryWithIndex-Size: "
//                    + gGeoHashLongs.size()+"#"+gPTRWithGeoHashAndSecondFiltering.size()
//                    +"%"+gPTRWithGeoHashAndDirectJudge.size()+"%"+gPTRWithGeoHashAndUDFFunction.size());
////            //相关结果写入文件操作，便于MatLab画图
//            String strSDTGeoHashThreeAll = searchDepthManual +" "
//                    +((endTimeQueryWithGeoHashAndSecondFiltering-startTimeQueryWithGeoHashAndSecondFiltering)/1000.0+" ")
//                    +((endTimeQueryWithGeoHashAndDirectJudge-startTimeQueryWithGeoHashAndDirectJudge)/1000.0+" ")
//                    +((endTimeQueryWithGeoHashAndUDFFunction-startTimeQueryWithGeoHashAndUDFFunction)/1000.0)+"\n";
//            File fileSDTGeoHashThreeAll = new File("rQS5_3minSDTGeoHashSDUMT30.txt");
//            try{
//                FileUtil.writeToFile(fileSDTGeoHashThreeAll, strSDTGeoHashThreeAll);
//            }catch (Exception e){
//                e.printStackTrace();
//            }
////
//            System.out.println("-----------------------------------------------------------------");
//        }
        //PhoenixSQLOperation.closeConnectionWithHBase();
    }
    public String toString(){
        return this.nameThread;
    }

    public static void main(String[] args){
        int sizeThread = 30;
        System.out.println("===============sizeThread: "+sizeThread+"================");
        for(int i=0;i<sizeThread;i++){
            GeoHashMainTestMultiThread tt = new GeoHashMainTestMultiThread("Thread-"+i);
            Thread t = new Thread(tt);
            t.start();
            System.out.println("-------------------"+t.toString()+"------------------");
        }
    }
}

