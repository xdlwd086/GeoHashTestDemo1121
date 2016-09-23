package cc.xidian.MainTest;

import cc.xidian.GeoHash.GeoHashConversion;
import cc.xidian.GeoObject.CircleQueryScope;
import cc.xidian.geoUtil.RandomOperation;

import java.util.BitSet;
import java.util.Random;

/**
 * Created by hadoop on 2016/9/8.
 */
public class RandomMainTest {
    public static void main(String[] args){
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

        BitSet b = new BitSet(60);
        b.set(0,55,false);
        System.out.println(b.toString());
    }
}
