package cc.xidian.GeoObject;

/**
 * Created by hadoop on 2016/9/17.
 */
public class CircleQueryScope {
    //public static final double PI = 3.1415926;
    public static final double RADIUS_EARTH = 6371.004;//地球半径，单位:km
    public double xLongitudeQuery;
    public double yLatitudeQuery;
    public double radiusQuery;//原型查询范围的半径，单位：km

    public CircleQueryScope(){
        this.xLongitudeQuery = 0;
        this.yLatitudeQuery = 0;
        this.radiusQuery = 0;
    }
    public CircleQueryScope(double xLongitudeQuery,double yLatitudeQuery,double radius){
        this.xLongitudeQuery = xLongitudeQuery;
        this.yLatitudeQuery = yLatitudeQuery;
        this.radiusQuery = radius;
    }
    public double getDistanceBetweenAnyPointAndQueryPoint(double xLongitudeAny,double yLatitudeAny){
        return (RADIUS_EARTH*Math.acos(Math.sin(yLatitudeQuery*(Math.PI/180))*Math.sin(yLatitudeAny*(Math.PI/180))+Math.cos(yLatitudeQuery*(Math.PI/180.0))
                *Math.cos(yLatitudeAny*(Math.PI/180.0))*Math.cos((xLongitudeAny-xLongitudeQuery)*(Math.PI/180.0)))*(Math.PI/180));
    }
    public double getDistanceBetweenAnyRecordAndQueryPoint(GeoPointTableRecord g){
        return (RADIUS_EARTH*Math.acos(Math.sin(yLatitudeQuery*(Math.PI/180))*Math.sin(g.yLatitude*(Math.PI/180))+Math.cos(yLatitudeQuery*(Math.PI/180.0))
                *Math.cos(g.yLatitude*(Math.PI/180.0))*Math.cos(Math.abs(g.xLongitude-xLongitudeQuery)*(Math.PI/180.0))));
    }
    public boolean isContainPoint(double xLongitudeAny,double yLatitudeAny){
        return getDistanceBetweenAnyPointAndQueryPoint(xLongitudeAny,yLatitudeAny)<=radiusQuery;
    }
    public boolean isContainGeoPointTableRecord(GeoPointTableRecord g){
        return getDistanceBetweenAnyRecordAndQueryPoint(g)<=radiusQuery;
    }
    public String toString(){
        return xLongitudeQuery+","+yLatitudeQuery+"#"+radiusQuery;
    }
}
