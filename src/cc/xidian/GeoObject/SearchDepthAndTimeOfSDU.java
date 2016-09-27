package cc.xidian.GeoObject;

/**
 * Created by hadoop on 2016/9/26.
 */
public class SearchDepthAndTimeOfSDU {
    //public int numOfThread;
    public int searchDepth;
    public double timeOfPhoenixGeoHashSecondFiltering;
    public double timeOfPhoenixGeoHashDirectJudge;
    public double timeOfPhoenixGeoHashUDFFunction;
    public SearchDepthAndTimeOfSDU(int searchDepth){
        //this.numOfThread = numOfThread;
        this.searchDepth = searchDepth;
        this.timeOfPhoenixGeoHashSecondFiltering = 0;
        this.timeOfPhoenixGeoHashDirectJudge = 0;
        this.timeOfPhoenixGeoHashUDFFunction = 0;
    }
    public SearchDepthAndTimeOfSDU(int searchDepth,double timeOfPhoenixGeoHashSecondFiltering,
                                   double timeOfPhoenixGeoHashDirectJudge,double timeOfPhoenixGeoHashUDFFunction){
        //this.numOfThread = numOfThread;
        this.searchDepth = searchDepth;
        this.timeOfPhoenixGeoHashSecondFiltering = timeOfPhoenixGeoHashSecondFiltering;
        this.timeOfPhoenixGeoHashDirectJudge = timeOfPhoenixGeoHashDirectJudge;
        this.timeOfPhoenixGeoHashUDFFunction = timeOfPhoenixGeoHashUDFFunction;
    }
    public String toString(){
        return searchDepth+" "+timeOfPhoenixGeoHashSecondFiltering+" "+timeOfPhoenixGeoHashDirectJudge+" "+timeOfPhoenixGeoHashUDFFunction;
    }
}
