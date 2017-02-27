package cc.xidian.MainTest;

import cc.xidian.GeoHash.GeoHashConversion;
import cc.xidian.GeoObject.CircleQueryScope;
import cc.xidian.geoUtil.FileUtil;
import cc.xidian.geoUtil.RandomOperation;
import com.vividsolutions.jts.geom.Polygon;

import java.io.File;
import java.util.BitSet;
import java.util.Random;

/**
 * Created by hadoop on 2016/9/8.
 */
public class RandomMainTest {
    public static void main(String[] args)throws Exception{
        //for(int i=0;i<1000;i++){
            //double temp = Math.round(Math.random()*360.0+(-180.0));
            //System.out.println(temp);
            //Random r = new Random();
            //System.out.println(RandomOperation.RandomDouble(-90.0,90.0));
        //}
        //long a = -17;
        //System.out.println(a>>>1);
//        String geoHashValueString = GeoHashConversion.encodeGeoHashFromLonAndLat(135.6324,-70.2278);
//        System.out.println(geoHashValueString);
//        double[] lonLat = GeoHashConversion.decodeLonAndLatFromGeoHash(geoHashValueString);
//        System.out.println(lonLat[0]+"#"+lonLat[1]);
//
//        long geoHashValueLong = GeoHashConversion.encodeGeoHashFromLonAndLatLong(135.6324,-70.2278);
//        System.out.println(geoHashValueLong);
//        System.out.println(Long.toBinaryString(geoHashValueLong));
//        double[] lonLatLong = GeoHashConversion.decodeLonAndLatFromGeoHashLong(geoHashValueLong);
//        System.out.println(lonLatLong[0]+"#"+lonLatLong[1]);
//
//        long geoHashValueLongZY = GeoHashConversion.LongLatToHash(135.6324,-70.2278);
//        System.out.println(geoHashValueLongZY);
//        System.out.println(Long.toBinaryString(geoHashValueLongZY));
//        double[] lonLatLongZY = GeoHashConversion.HashToLongLat(geoHashValueLongZY);
//        System.out.println(lonLatLongZY[0]+"#"+lonLatLongZY[1]);

        //原型查询范围的测试
//        CircleQueryScope cQS = new CircleQueryScope(20.0,15.0,10.0);
//        double xLongitudeAny = 20.0;
//        double yLatitudeAny = 10.0;
//        double C = Math.sin(cQS.yLatitudeQuery)*Math.sin(yLatitudeAny)*Math.cos(cQS.xLongitudeQuery - xLongitudeAny)+Math.cos(cQS.yLatitudeQuery)*Math.cos(yLatitudeAny);
//        double distance = cQS.RADIUS_EARTH*Math.acos(C)*Math.PI/180;
//        System.out.println(distance);
//        System.out.println(cQS.getDistanceBetweenAnyPointAndQueryPoint(xLongitudeAny,yLatitudeAny));
//        System.out.println(cQS.isContainPoint(xLongitudeAny,yLatitudeAny));

        //BitSet测试
        for (int i = 1201;i<=1231; i++){
            File fileSDTGeoHashSDUMT = new File("data"+File.separator+"2016"+i+".export.CSV");
//            File fileSDTGeoHashSDUMT = new File("20130401_20131231.GDELTEventRecordsSimpleAsWithGeoHashValue.csv");
            File fileSDTGeoHashSDTMTAverage = new File("20160101_20161231.GDELTEventRecordsSimpleAsWithGeoHashValueNoReplicate.csv");
            FileUtil.getFileGDELTEventRecordSimpleAsFromFileGDELTEventDataSource(fileSDTGeoHashSDUMT,fileSDTGeoHashSDTMTAverage);
        }
//        File fileSDTGeoHashSDUMT = new File("rQS1_1minSDTGeoHashSDU100SumMergeAreaRatio100MNew08.txt");
//        File fileSDTGeoHashSDTMTAverage = new File("rQS1_1minSDTGeoHashSDU100SumMergeAreaRatio100MNew08_P.txt");
//        FileUtil.getFileSDTGeoHashSDUMTAverageFromFileInitial(fileSDTGeoHashSDUMT,fileSDTGeoHashSDTMTAverage);
//        double d = 12.56397*100000;
//        System.out.println(d);

//        File fileGDELTEventRecordSimpleAs = new File("20161201_20161231.GDELTEventRecordsSimpleAsWithGeoHashValueNoReplicate.csv");
//        File fileGDELTEventRecordXY = new File("20161201_20161231.GDELTEventRecordsXYNoReplicate.csv");
//        FileUtil.getFileGDELTEventRecordXYsFromGDELTEventRecordSimples(fileGDELTEventRecordSimpleAs,fileGDELTEventRecordXY);

//        System.out.println(ClassLoader.getSystemClassLoader());
        //Polygon g = new Polygon()
    }
}
