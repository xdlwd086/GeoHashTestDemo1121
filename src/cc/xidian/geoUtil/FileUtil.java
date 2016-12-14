package cc.xidian.geoUtil;

import cc.xidian.GeoHash.GeoHashConversion;
import cc.xidian.GeoObject.GDELTEventRecordSimpleA;
import cc.xidian.GeoObject.SearchDepthAndTimeOfSDU;

import java.io.*;
import java.text.DecimalFormat;
import java.util.ArrayList;

/**
 * Created by hadoop on 2016/9/8.
 */
public class FileUtil {
    public static void writeGeoPointTableRecordsToFile(File fileGeoPointTableRecords,String str) throws Exception{
        FileWriter fileWriter = new FileWriter(fileGeoPointTableRecords,true);//以追加方式写入文件
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        bufferedWriter.write(str);
        bufferedWriter.close();
        fileWriter.close();
    }
    public static void writeToFile(File fileGeoPointTableRecords,String str) throws Exception{
        FileWriter fileWriter = new FileWriter(fileGeoPointTableRecords,true);//以追加方式写入文件
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        bufferedWriter.write(str);
        bufferedWriter.close();
        fileWriter.close();
    }

    /**
     * 函数功能：文本处理，求和并平均，编写时间：2016年9月26日23:18:39
     * @param fileSDTGeoHashSDUMT 原始文件
     * @param fileSDTGeoHashSDUMTAverage 结果文件
     * @throws Exception
     */
    public static void getFileSDTGeoHashSDUMTAverageFromFileInitial(File fileSDTGeoHashSDUMT,File fileSDTGeoHashSDUMTAverage)throws Exception{
        SearchDepthAndTimeOfSDU[] sDTArray = new SearchDepthAndTimeOfSDU[1200];
        int sum = 300;
        //初始化操作
        for(int i=1;i<=29;i++){
            sDTArray[i] = new SearchDepthAndTimeOfSDU(i);
        }
        if(fileSDTGeoHashSDUMT.exists()&&fileSDTGeoHashSDUMT.isFile()){
            InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(fileSDTGeoHashSDUMT),"UTF-8");
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            String strLine;
            String[] strLineArray;
            while((strLine = bufferedReader.readLine())!=null){
                strLineArray = strLine.split(" ");//获取每一行的各个字段内容
                //实例化一个记录对象
                SearchDepthAndTimeOfSDU s = new SearchDepthAndTimeOfSDU(Integer.parseInt(strLineArray[0]),
                        Double.parseDouble(strLineArray[1]),Double.parseDouble(strLineArray[2]),Double.parseDouble(strLineArray[3]));
                //三个时间求和操作，借助了计数排序的思想
                sDTArray[s.searchDepth].timeOfPhoenixGeoHashSecondFiltering += s.timeOfPhoenixGeoHashSecondFiltering;
                sDTArray[s.searchDepth].timeOfPhoenixGeoHashDirectJudge += s.timeOfPhoenixGeoHashDirectJudge;
                sDTArray[s.searchDepth].timeOfPhoenixGeoHashUDFFunction += s.timeOfPhoenixGeoHashUDFFunction;
            }
            bufferedReader.close();
            inputStreamReader.close();
        }
        DecimalFormat df = new DecimalFormat("#.000");
        for(int i=1;i<=29;i++){
            sDTArray[i].searchDepth = i;
            //三个时间求和后取平均值，并保留小数点后三位数据
            sDTArray[i].timeOfPhoenixGeoHashSecondFiltering /= sum;
            sDTArray[i].timeOfPhoenixGeoHashSecondFiltering = Double.parseDouble(df.format(sDTArray[i].timeOfPhoenixGeoHashSecondFiltering));
            sDTArray[i].timeOfPhoenixGeoHashDirectJudge /= sum;
            sDTArray[i].timeOfPhoenixGeoHashDirectJudge = Double.parseDouble(df.format(sDTArray[i].timeOfPhoenixGeoHashDirectJudge));
            sDTArray[i].timeOfPhoenixGeoHashUDFFunction /= sum;
            sDTArray[i].timeOfPhoenixGeoHashUDFFunction = Double.parseDouble(df.format(sDTArray[i].timeOfPhoenixGeoHashUDFFunction));
            System.out.println(sDTArray[i].toString());//输出打印操作
            //写入文件操作
            FileWriter fileWriter = new FileWriter(fileSDTGeoHashSDUMTAverage,true);//以追加方式写入文件
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            bufferedWriter.write(sDTArray[i].toString()+"\n");
            bufferedWriter.close();
            fileWriter.close();
        }
    }

    public static void getCombinedTimeFileFromPhoenixTimeFileAndPostgreSQLTimeFile(File filePhoenix,File filePostgreSQL,File fileCombined){
        try{
            InputStreamReader inputStreamReaderPh = new InputStreamReader(new FileInputStream(filePhoenix),"UTF-8");
            InputStreamReader inputStreamReaderPg = new InputStreamReader(new FileInputStream(filePostgreSQL),"UTF-8");
            BufferedReader bufferedReaderPh = new BufferedReader(inputStreamReaderPh);
            BufferedReader bufferedReaderPg = new BufferedReader(inputStreamReaderPg);
            String strLinePh;
            String strLinePg;
            String[] strLineArrayPh,strLineArrayPg;
            while((strLinePh = bufferedReaderPh.readLine())!=null&&(strLinePg = bufferedReaderPg.readLine())!=null) {
                strLineArrayPh = strLinePh.split(" ");
                strLineArrayPg = strLinePg.split(" ");
                if(strLineArrayPh[0].equals(strLineArrayPg[0])){
                    String strLineCombined = strLinePh+" "+strLineArrayPg[1];
                    System.out.println(strLineCombined);
                    FileWriter fileWriter = new FileWriter(fileCombined,true);//以追加方式写入文件
                    BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
                    bufferedWriter.write(strLineCombined+"\n");
                    bufferedWriter.close();
                    fileWriter.close();
                }

            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }
    public static void getCombinedTimeFileFromPhoenixTimeFileAndPostgreSQLTimeFile(File filePhoenix,File filePgBTree,File filePgGist,File fileCombined){
        try{
            InputStreamReader inputStreamReaderPh = new InputStreamReader(new FileInputStream(filePhoenix),"UTF-8");
            InputStreamReader inputStreamReaderPgBTree = new InputStreamReader(new FileInputStream(filePgBTree),"UTF-8");
            InputStreamReader inputStreamReaderPgGist = new InputStreamReader(new FileInputStream(filePgGist),"UTF-8");
            BufferedReader bufferedReaderPh = new BufferedReader(inputStreamReaderPh);
            BufferedReader bufferedReaderPgBTree = new BufferedReader(inputStreamReaderPgBTree);
            BufferedReader bufferedReaderPgGist = new BufferedReader(inputStreamReaderPgGist);
            String strLinePh;
            String strLinePgBTree;
            String strLinePgGist;
            String[] strLineArrayPh,strLineArrayPgBTree,strLineArrayPgGist;
            while((strLinePh = bufferedReaderPh.readLine())!=null&&(strLinePgBTree = bufferedReaderPgBTree.readLine())!=null&&(strLinePgGist = bufferedReaderPgGist.readLine())!=null) {
                strLineArrayPh = strLinePh.split(" ");
                strLineArrayPgBTree = strLinePgBTree.split(" ");
                strLineArrayPgGist = strLinePgGist.split(" ");
                if(strLineArrayPh[0].equals(strLineArrayPgBTree[0])&&strLineArrayPh[0].equals(strLineArrayPgGist[0])){
                    String strLineCombined = strLinePh+" "+strLineArrayPgBTree[1]+" "+strLineArrayPgGist[1];
                    System.out.println(strLineCombined);
                    FileWriter fileWriter = new FileWriter(fileCombined,true);//以追加方式写入文件
                    BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
                    bufferedWriter.write(strLineCombined+"\n");
                    bufferedWriter.close();
                    fileWriter.close();
                }

            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }
    /**
     * 函数功能：文本处理，求和并平均，编写时间：2016年9月26日23:18:39
     * @param fileGDELTEventDataSource 原始文件
     * @param fileGDELTEventRecordsSimpleAs 结果文件
     * @throws Exception
     */
    public static void getFileGDELTEventRecordSimpleAsFromFileGDELTEventDataSource(File fileGDELTEventDataSource,File fileGDELTEventRecordsSimpleAs)throws Exception{
        int count = 0;
        FileWriter fileWriter = new FileWriter(fileGDELTEventRecordsSimpleAs,true);//以追加方式写入文件
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        //if(fileGDELTEventDataSource.exists()&&fileGDELTEventDataSource.isFile()){
            InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(fileGDELTEventDataSource),"UTF-8");
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            String strLine;
            String[] strLineArray;
            while((strLine = bufferedReader.readLine())!=null) {
                strLineArray = strLine.split("\t");//获取每一行的各个字段内容
                GDELTEventRecordSimpleA g = new GDELTEventRecordSimpleA();
                if (!strLineArray[strLineArray.length - 1].isEmpty() && !strLineArray[strLineArray.length - 2].isEmpty() && !strLineArray[strLineArray.length - 3].isEmpty() &&
                        !strLineArray[strLineArray.length - 4].isEmpty() && !strLineArray[strLineArray.length - 5].isEmpty() && !strLineArray[strLineArray.length - 6].isEmpty() &&
                        !strLineArray[strLineArray.length - 7].isEmpty() && !strLineArray[strLineArray.length - 9].isEmpty() && !strLineArray[0].isEmpty() &&
                        !strLineArray[1].isEmpty() && !strLineArray[7].isEmpty()) {
                    g.globalEventID = Integer.parseInt(strLineArray[0]);
                    g.sqlDate = Integer.parseInt(strLineArray[1]);
                    g.actor1Code = strLineArray[5];
                    g.actor1Name = strLineArray[6];
                    g.actor1CountryCode = strLineArray[7];

                    g.actionGeo_Type = Integer.parseInt(strLineArray[strLineArray.length - 9]);
                    //g.actionGeo_FullName = strLineArray[strLineArray.length-8];
                    g.actionGeo_CountryCode = strLineArray[strLineArray.length - 7];
                    g.actionGeo_ADM1Code = strLineArray[strLineArray.length-6];
                    g.actionGeo_Lat = Double.parseDouble(strLineArray[strLineArray.length - 5]);
                    g.actionGeo_Long = Double.parseDouble(strLineArray[strLineArray.length - 4]);
                    g.actionGeo_GeoHashValue = GeoHashConversion.LongLatToHash(g.actionGeo_Long,g.actionGeo_Lat);//计算GeoHash值，64位long类型
                    g.actionGeo_FeatureID = strLineArray[strLineArray.length-3];
                    g.dateAdded = Integer.parseInt(strLineArray[strLineArray.length - 2]);
                    g.sourceURL = strLineArray[strLineArray.length - 1];

                    System.out.println(count+","+g.toString());
                    bufferedWriter.write(g.toString() + "\n");
                    count++;
                }
            }
            bufferedReader.close();
            inputStreamReader.close();
            bufferedWriter.close();
            fileWriter.close();
    }

    public static void getFileGDELTEventRecordSimpleAsSumFromFileGDELTEventRecordSimpleAs(File fileGDELTEventRecordSimpleAs,File fileGDELTEventRecordsSimpleAsSum)throws Exception{
        int count = 0;
        FileWriter fileWriter = new FileWriter(fileGDELTEventRecordsSimpleAsSum,true);//以追加方式写入文件
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        //if(fileGDELTEventDataSource.exists()&&fileGDELTEventDataSource.isFile()){
        InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(fileGDELTEventRecordSimpleAs),"UTF-8");
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        String strLine;
        String[] strLineArray;
        while((strLine = bufferedReader.readLine())!=null) {
//            strLineArray = strLine.split("\t");//获取每一行的各个字段内容
//            GDELTEventRecordSimpleA g = new GDELTEventRecordSimpleA();
//            if (!strLineArray[strLineArray.length - 1].isEmpty() && !strLineArray[strLineArray.length - 2].isEmpty() && !strLineArray[strLineArray.length - 3].isEmpty() &&
//                    !strLineArray[strLineArray.length - 4].isEmpty() && !strLineArray[strLineArray.length - 5].isEmpty() && !strLineArray[strLineArray.length - 6].isEmpty() &&
//                    !strLineArray[strLineArray.length - 7].isEmpty() && !strLineArray[strLineArray.length - 9].isEmpty() && !strLineArray[0].isEmpty() &&
//                    !strLineArray[1].isEmpty() && !strLineArray[7].isEmpty()) {
//                g.globalEventID = Integer.parseInt(strLineArray[0]);
//                g.sqlDate = Integer.parseInt(strLineArray[1]);
//                g.actor1Code = strLineArray[5];
//                g.actor1Name = strLineArray[6];
//                g.actor1CountryCode = strLineArray[7];
//
//                g.actionGeo_Type = Integer.parseInt(strLineArray[strLineArray.length - 9]);
//                //g.actionGeo_FullName = strLineArray[strLineArray.length-8];
//                g.actionGeo_CountryCode = strLineArray[strLineArray.length - 7];
//                g.actionGeo_ADM1Code = strLineArray[strLineArray.length-6];
//                g.actionGeo_Lat = Double.parseDouble(strLineArray[strLineArray.length - 5]);
//                g.actionGeo_Long = Double.parseDouble(strLineArray[strLineArray.length - 4]);
//                g.actionGeo_GeoHashValue = GeoHashConversion.LongLatToHash(g.actionGeo_Long,g.actionGeo_Lat);//计算GeoHash值，64位long类型
//                g.actionGeo_FeatureID = strLineArray[strLineArray.length-3];
//                g.dateAdded = Integer.parseInt(strLineArray[strLineArray.length - 2]);
//                g.sourceURL = strLineArray[strLineArray.length - 1];
//
                System.out.println(count+","+strLine);
                bufferedWriter.write(strLine + "\n");
                count++;
            //}
        }
        bufferedReader.close();
        inputStreamReader.close();
        bufferedWriter.close();
        fileWriter.close();
    }
}
