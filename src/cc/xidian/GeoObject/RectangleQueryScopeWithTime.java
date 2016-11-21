package cc.xidian.GeoObject;

/**
 * Created by hadoop on 2016/9/9.
 */
public class RectangleQueryScopeWithTime {
    public RectangleQueryScope rQS;
    public int startDateDay;
    public int endDateDay;

    public RectangleQueryScopeWithTime(){
        this.rQS = new RectangleQueryScope();
        this.startDateDay = 0;
        this.endDateDay = 0;
    }
    public void RectangleQueryScopeDelta(double xLongitudeBL,double yLatitudeBL,double deltaX,double deltaY,int startDateDay,int endDateDay){
        this.rQS.RectangleQueryScopeDelta(xLongitudeBL,yLatitudeBL,deltaX,deltaY);
        this.startDateDay = startDateDay;
        this.endDateDay = endDateDay;
    }
    public RectangleQueryScopeWithTime(double xLongitudeBL, double yLatitudeBL, double xLongitudeTR, double yLatitudeTR,int startDateDay,int endDateDay){
        this.rQS = new RectangleQueryScope(xLongitudeBL,yLatitudeBL,xLongitudeTR,yLatitudeTR);
        this.startDateDay = startDateDay;
        this.endDateDay = endDateDay;
    }
    public boolean isContainPoint(double xLongitude,double yLatitude){
        return (xLongitude>=this.rQS.xLongitudeBL&&xLongitude<=this.rQS.xLongitudeTR&&yLatitude>=this.rQS.yLatitudeBL&&yLatitude<=this.rQS.yLatitudeTR);
    }
    public boolean isContainGeoPointTableRecord(GeoPointTableRecord g){
        return (g.xLongitude>=this.rQS.xLongitudeBL&&g.xLongitude<=this.rQS.xLongitudeTR&&g.yLatitude>=this.rQS.yLatitudeBL&&g.yLatitude<=this.rQS.yLatitudeTR);
    }
    public boolean isContainGeoPointTableRecord(GeoPointTableRecordSimple g){
        return (g.xLongitude>=this.rQS.xLongitudeBL&&g.xLongitude<=this.rQS.xLongitudeTR&&g.yLatitude>=this.rQS.yLatitudeBL&&g.yLatitude<=this.rQS.yLatitudeTR);
    }
    public boolean isContainRectangleQueryScope(RectangleQueryScopeWithTime rQST){
        return (this.rQS.xLongitudeBL<=rQST.rQS.xLongitudeBL&&this.rQS.xLongitudeTR>=rQST.rQS.xLongitudeTR&&
                this.rQS.yLatitudeBL<=rQST.rQS.yLatitudeBL&&this.rQS.yLatitudeTR>=rQST.rQS.yLatitudeTR);
    }
    public boolean isIntersectWithRectangleQueryScope(RectangleQueryScopeWithTime rQST){
        return !(this.rQS.xLongitudeBL>=rQST.rQS.xLongitudeTR||this.rQS.xLongitudeTR<=rQST.rQS.xLongitudeBL
                ||this.rQS.yLatitudeBL>=rQST.rQS.yLatitudeTR||this.rQS.yLatitudeTR<rQST.rQS.yLatitudeBL);
    }
    public String toString(){
        return rQS.toString()+","+this.startDateDay+","+this.endDateDay;
    }

}
