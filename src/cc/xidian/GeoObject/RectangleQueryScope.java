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
    public double deltaX;
    public double deltaY;
    //public String geoHashValueBL;
    //public String geoHashValueTR;

    public RectangleQueryScope(){
        this.xLongitudeBL = 0;
        this.yLatitudeBL = 0;
        this.deltaX = 0;
        this.deltaY = 0;
        this.xLongitudeTR = this.xLongitudeBL + this.deltaX;
        this.yLatitudeTR = this.yLatitudeBL + this.deltaY;
        //this.geoHashValueBL = "";
        //this.geoHashValueTR = "";
    }
    public void RectangleQueryScopeDelta(double xLongitudeBL,double yLatitudeBL,double deltaX,double deltaY){
        this.xLongitudeBL = xLongitudeBL;
        this.yLatitudeBL = yLatitudeBL;
        this.deltaX = deltaX;
        this.deltaY = deltaY;
        this.xLongitudeTR = xLongitudeBL + deltaX;
        this.yLatitudeTR = yLatitudeBL +deltaY;
        //this.geoHashValueBL = GeoHashConversion.encodeGeoHashFromLonAndLat(xLongitudeBL,yLatitudeBL);
        //this.geoHashValueTR = GeoHashConversion.encodeGeoHashFromLonAndLat((xLongitudeBL+deltaX),(yLatitudeBL+deltaY));
    }
    public RectangleQueryScope(double xLongitudeBL,double yLatitudeBL,double xLongitudeTR,double yLatitudeTR){
        this.xLongitudeBL = xLongitudeBL;
        this.yLatitudeBL = yLatitudeBL;
        this.deltaX = xLongitudeTR - xLongitudeBL;
        this.deltaY = yLatitudeTR - yLatitudeBL;
        this.xLongitudeTR = xLongitudeTR;
        this.yLatitudeTR = yLatitudeTR;
        //this.geoHashValueBL = GeoHashConversion.encodeGeoHashFromLonAndLat(xLongitudeBL,yLatitudeBL);
        //this.geoHashValueTR = GeoHashConversion.encodeGeoHashFromLonAndLat(xLongitudeTR,yLatitudeTR);
    }
    public boolean isContainPoint(double xLongitude,double yLatitude){
        return (xLongitude>=this.xLongitudeBL&&xLongitude<=this.xLongitudeTR&&yLatitude>=yLatitudeBL&&yLatitude<=yLatitudeTR);
    }
    public boolean isContainGeoPointTableRecord(GeoPointTableRecord g){
        return (g.xLongitude>=this.xLongitudeBL&&g.xLongitude<=this.xLongitudeTR&&g.yLatitude>=yLatitudeBL&&g.yLatitude<=yLatitudeTR);
    }
    public boolean isContainGeoPointTableRecord(GeoPointTableRecordSimple g){
        return (g.xLongitude>=this.xLongitudeBL&&g.xLongitude<=this.xLongitudeTR&&g.yLatitude>=yLatitudeBL&&g.yLatitude<=yLatitudeTR);
    }
    public boolean isContainRectangleQueryScope(RectangleQueryScope rQS){
        return (this.xLongitudeBL<=rQS.xLongitudeBL&&this.xLongitudeTR>=rQS.xLongitudeTR&&
                this.yLatitudeBL<=rQS.yLatitudeBL&&this.yLatitudeTR>=rQS.yLatitudeTR);
    }
    public boolean isIntersectWithRectangleQueryScope(RectangleQueryScope rQS){
        return !(this.xLongitudeBL>=rQS.xLongitudeTR||this.xLongitudeTR<=rQS.xLongitudeBL
                ||this.yLatitudeBL>=rQS.yLatitudeTR||this.yLatitudeTR<rQS.yLatitudeBL);
    }
    public String toString(){
        return this.xLongitudeBL+","+this.yLatitudeBL+"#"+this.xLongitudeTR+","+this.yLatitudeTR;
    }

}
