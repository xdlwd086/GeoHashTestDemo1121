package cc.xidian.geoUtil;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

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
}
