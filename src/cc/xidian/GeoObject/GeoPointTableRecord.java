package cc.xidian.GeoObject;

/**
 * Created by hadoop on 2016/9/8.
 */
public class GeoPointTableRecord {
    public int geoID;//从0开始递增的整数
    public String geoName;//地名，随机字符串，长度为6
    public double xLongitude;//经度，保留小数点后4位
    public double yLatitude;//纬度，保留小数点后4位
    public String geoHashValue;//GeoHash值,Base32编码
    public long geoHashValueLong;//GeoHash值，long类型

    public GeoPointTableRecord(){
        this.geoID = 0;
        this.geoName = "";
        this.xLongitude = 0;
        this.yLatitude = 0;
        this.geoHashValue = "";
        this.geoHashValueLong = 0;
    }
    public GeoPointTableRecord(int geoID,String geoName,double xLongitude,double yLatitude,String geoHashValue,long geoHashValueLong){
        this.geoID = geoID;
        this.geoName = geoName;
        this.xLongitude = xLongitude;
        this.yLatitude = yLatitude;
        this.geoHashValue = geoHashValue;
        this.geoHashValueLong = geoHashValueLong;

    }
    public String toString(){
        return geoID+"#"+geoName+"#"+xLongitude+","+yLatitude+"#"+geoHashValue+"#"+geoHashValueLong;
    }
}
