package cc.xidian.GeoObject;

/**
 * Created by hadoop on 2016/9/8.
 */
public class GeoPointTableRecordSimple {
    public int geoID;//从0开始递增的整数
    public String geoName;//地名，随机字符串，长度为6
    public double xLongitude;//经度，保留小数点后4位
    public double yLatitude;//纬度，保留小数点后4位
    public long geoHashValueLong;//GeoHash值，long类型,64位二进制位

    public GeoPointTableRecordSimple(){
        this.geoID = 0;
        this.geoName = "";
        this.xLongitude = 0;
        this.yLatitude = 0;
        this.geoHashValueLong = 0;
    }
    public GeoPointTableRecordSimple(int geoID, String geoName, double xLongitude, double yLatitude, long geoHashValueLong){
        this.geoID = geoID;
        this.geoName = geoName;
        this.xLongitude = xLongitude;
        this.yLatitude = yLatitude;
        this.geoHashValueLong = geoHashValueLong;
    }
    public String toString(){
        return geoID+","+geoName+","+xLongitude+","+yLatitude+","+geoHashValueLong;
    }

}
