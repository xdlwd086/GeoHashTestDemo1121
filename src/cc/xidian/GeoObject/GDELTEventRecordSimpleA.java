package cc.xidian.GeoObject;

/**
 * Created by hadoop on 2016/11/9.
 */
public class GDELTEventRecordSimpleA {
    /**
     * GDELT事件唯一标识
     */
    public int globalEventID;
    /**
     * 事件发生的日期，格式为YYYYMMDD
     */
    public int sqlDate;
    /**
     * Actor1的CAMEO编码
     */
    public String actor1Code;
    /**
     * Actor1的名称
     */
    public String actor1Name;
    /**
     * Actor1的国家编码
     */
    public String actor1CountryCode;
    /**
     * Action的地理类型
     */
    public int actionGeo_Type;
    /**
     * Action的地理国家编码
     */
    public String actionGeo_CountryCode;
    /**
     * Action的ADM1编码
     */
    public String actionGeo_ADM1Code;
    /**
     * 经度
     */
    public double actionGeo_Long;//经度
    /**
     * 纬度
     */
    public double actionGeo_Lat;//纬度
    /**
     * 经纬度对应的GeoHash值，由经纬度计算得出，并不是数据文件中原有的
     */
    public long actionGeo_GeoHashValue;
    /**
     * Action的地理要素ID
     */
    public String actionGeo_FeatureID;
    /**
     * 记录被加入数据库的时间
     */
    public int dateAdded;
    /**
     * 事件的URL
     */
    public String sourceURL;

    /**
     * 无参构造函数，类中属性默认初始化
     */
    public GDELTEventRecordSimpleA(){
        this.globalEventID = 0;
        this.sqlDate = 0;
        this.actor1Code = "";
        this.actor1CountryCode = "";
        this.actor1Name = "";
        this.actionGeo_Type = 0;
        this.actionGeo_CountryCode = "";
        this.actionGeo_ADM1Code = "";
        this.actionGeo_Lat = 0;
        this.actionGeo_Long = 0;
        this.actionGeo_GeoHashValue = 0;
        this.actionGeo_FeatureID = "";
        this.dateAdded = 0;
        this.sourceURL = "";
    }

    /**
     * 含参构造函数
     * @param globalEventID 事件唯一ID
     * @param sqlDate 事件发生的日期
     * @param actor1Code Actor1的CAMEO编码
     * @param actor1Name Actor1的名称
     * @param actor1CountryCode Actor1的地理国家编码
     * @param actionGeo_Type Action的地理类型
     * @param actionGeo_CountryCode Action的地理国家编码
     * @param actionGeo_ADM1Code Action的ADM1编码
     * @param actionGeo_Long Action的经度
     * @param actionGeo_Lat Action的纬度
     * @param actionGeo_GeoHashValue Action的经纬度对应的geohash值，计算得出
     * @param actionGeo_FeatureID Action的要素ID
     * @param dateAdded 记录加入到数据库的时间
     * @param sourceURL 事件的URL
     */
    public GDELTEventRecordSimpleA(int globalEventID,int sqlDate,String actor1Code,String actor1Name,String actor1CountryCode,
                                   int actionGeo_Type,String actionGeo_CountryCode,String actionGeo_ADM1Code,double actionGeo_Long,
                                   double actionGeo_Lat,long actionGeo_GeoHashValue,String actionGeo_FeatureID,
                                   int dateAdded,String sourceURL){
        this.globalEventID = globalEventID;
        this.sqlDate = sqlDate;
        this.actor1Code = actor1Code;
        this.actor1Name = actor1Name;
        this.actor1CountryCode = actor1CountryCode;
        this.actionGeo_Type = actionGeo_Type;
        this.actionGeo_CountryCode = actionGeo_CountryCode;
        this.actionGeo_ADM1Code = actionGeo_ADM1Code;
        this.actionGeo_Long = actionGeo_Long;
        this.actionGeo_Lat = actionGeo_Lat;
        this.actionGeo_GeoHashValue = actionGeo_GeoHashValue;
        this.actionGeo_FeatureID = actionGeo_FeatureID;
        this.dateAdded = dateAdded;
        this.sourceURL = sourceURL;
    }

    /**
     * 自定义的toString方法
     * @return 属性方法字符串
     */
    @Override
    public String toString(){
        return this.globalEventID+","+this.sqlDate+","+this.actor1Code+","+this.actor1Name+","+this.actionGeo_CountryCode+","
                +this.actionGeo_Type+","+this.actionGeo_CountryCode+","+this.actionGeo_ADM1Code+","
                +this.actionGeo_Long+","+this.actionGeo_Lat+","+this.actionGeo_GeoHashValue+","+this.actionGeo_FeatureID+","
                +this.dateAdded+","+this.sourceURL;
    }
}
