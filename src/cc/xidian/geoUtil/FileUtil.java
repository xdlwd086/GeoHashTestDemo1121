package cc.xidian.geoUtil;

import cc.xidian.GeoHash.GeoHashConversion;
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
        SearchDepthAndTimeOfSDU[] sDTArray = new SearchDepthAndTimeOfSDU[21];
        //初始化操作
        for(int i=1;i<=GeoHashConversion.SEARCH_DEPTH;i++){
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
        for(int i=1;i<=GeoHashConversion.SEARCH_DEPTH;i++){
            sDTArray[i].searchDepth = i;
            //三个时间求和后取平均值，并保留小数点后三位数据
            sDTArray[i].timeOfPhoenixGeoHashSecondFiltering /= GeoHashConversion.SEARCH_DEPTH;
            sDTArray[i].timeOfPhoenixGeoHashSecondFiltering = Double.parseDouble(df.format(sDTArray[i].timeOfPhoenixGeoHashSecondFiltering));
            sDTArray[i].timeOfPhoenixGeoHashDirectJudge /= GeoHashConversion.SEARCH_DEPTH;
            sDTArray[i].timeOfPhoenixGeoHashDirectJudge = Double.parseDouble(df.format(sDTArray[i].timeOfPhoenixGeoHashDirectJudge));
            sDTArray[i].timeOfPhoenixGeoHashUDFFunction /= GeoHashConversion.SEARCH_DEPTH;
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
}
