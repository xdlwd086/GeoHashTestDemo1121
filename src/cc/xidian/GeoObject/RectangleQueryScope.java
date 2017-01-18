package cc.xidian.GeoObject;

import cc.xidian.GeoHash.GeoHashConversion;

/**
 * Created by hadoop on 2016/9/9.
 */
public class RectangleQueryScope {
    public double xLongitudeBL;
    public double yLatitudeBL;
    public double xLongitudeTR;
    public double yLatitudeTR;
    public double xLongitudeCenter;
    public double yLatitudeCenter;
    public double deltaX;
    public double deltaY;

    public RectangleQueryScope(){
        this.xLongitudeBL = 0;
        this.yLatitudeBL = 0;
        this.xLongitudeCenter = 0;
        this.yLatitudeCenter = 0;
        this.deltaX = 0;
        this.deltaY = 0;
        this.xLongitudeTR = this.xLongitudeBL + this.deltaX;
        this.yLatitudeTR = this.yLatitudeBL + this.deltaY;
    }
    public void RectangleQueryScopeDelta(double xLongitudeBL,double yLatitudeBL,double deltaX,double deltaY){
        this.xLongitudeBL = xLongitudeBL;
        this.yLatitudeBL = yLatitudeBL;
        this.deltaX = deltaX;
        this.deltaY = deltaY;
        this.xLongitudeTR = xLongitudeBL + deltaX;
        this.yLatitudeTR = yLatitudeBL +deltaY;
        this.xLongitudeCenter = xLongitudeBL + deltaX/2.0;
        this.yLatitudeCenter = yLatitudeBL + deltaY/2.0;
    }
    public RectangleQueryScope(double xLongitudeBL,double yLatitudeBL,double xLongitudeTR,double yLatitudeTR){
        this.xLongitudeBL = xLongitudeBL;
        this.yLatitudeBL = yLatitudeBL;
        this.deltaX = xLongitudeTR - xLongitudeBL;
        this.deltaY = yLatitudeTR - yLatitudeBL;
        this.xLongitudeTR = xLongitudeTR;
        this.yLatitudeTR = yLatitudeTR;
        this.xLongitudeCenter = (xLongitudeBL+xLongitudeTR)/2.0;
        this.yLatitudeCenter = (yLatitudeBL+yLatitudeTR)/2.0;
    }
    public boolean isContainPoint(double xLongitude,double yLatitude){
        return (xLongitude>=this.xLongitudeBL&&xLongitude<=this.xLongitudeTR&&yLatitude>=yLatitudeBL&&yLatitude<=yLatitudeTR);
    }
    public boolean isContainGeoPointTableRecord(GeoPointTableRecord g){
        return (g.xLongitude>=this.xLongitudeBL&&g.xLongitude<=this.xLongitudeTR&&g.yLatitude>=yLatitudeBL&&g.yLatitude<=yLatitudeTR);
    }
    public boolean isContainGeoPointTableRecord(GeoPointTableRecordSimple g){
        return (g.xLongitude>=this.xLongitudeBL&&g.xLongitude<=this.xLongitudeTR&&g.yLatitude>=this.yLatitudeBL&&g.yLatitude<=this.yLatitudeTR);
    }
    public boolean isContainRectangleQueryScope(RectangleQueryScope rQS){
        return (this.xLongitudeBL<=rQS.xLongitudeBL&&this.xLongitudeTR>=rQS.xLongitudeTR&&
                this.yLatitudeBL<=rQS.yLatitudeBL&&this.yLatitudeTR>=rQS.yLatitudeTR);
    }
    public boolean isIntersectWithRectangleQueryScope(RectangleQueryScope rQS){
        return !(this.xLongitudeBL>rQS.xLongitudeTR||this.xLongitudeTR<rQS.xLongitudeBL
                ||this.yLatitudeBL>rQS.yLatitudeTR||this.yLatitudeTR<rQS.yLatitudeBL);
    }
    public String toString(){
        return this.xLongitudeBL+","+this.yLatitudeBL+"#"+this.xLongitudeTR+","+this.yLatitudeTR;
    }

}
