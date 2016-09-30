package cc.xidian.GeoObject;

import cc.xidian.GeoHash.RectanglePrefix;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Stack;

/**
 * Created by hadoop on 2016/9/30.
 */
public class GeoHashIndexRecord {
    public int searchDepth;
    public double areaRatio;
    public int sizeOfRectanglePrefix;
    public int sizeOfGeoHashLongs;
    public RectangleQueryScope rQS;
    public ArrayList<RectanglePrefix> rPArray;
    public Stack<long[]> sGeoHashLongs;

    public GeoHashIndexRecord(){
        this.searchDepth = 0;
        this.areaRatio = 0;
        this.rQS = new RectangleQueryScope();
        this.rPArray = new ArrayList<RectanglePrefix>();
        this.sizeOfRectanglePrefix = this.rPArray.size();
        this.sGeoHashLongs = new Stack<long[]>();
        this.sizeOfGeoHashLongs = this.sGeoHashLongs.size();
    }
    public GeoHashIndexRecord(int searchDepth,double areaRatio,RectangleQueryScope rQS){
        this.searchDepth = searchDepth;
        this.areaRatio = areaRatio;
        this.rQS = rQS;
        this.rPArray = new ArrayList<RectanglePrefix>();
        this.sizeOfRectanglePrefix = this.rPArray.size();
        this.sGeoHashLongs = new Stack<long[]>();
        this.sizeOfGeoHashLongs = this.sGeoHashLongs.size();
    }
    public void getMergedGeoHashLongsFromRectanglePrefixArray(){
        for(RectanglePrefix r: this.rPArray){
            long[] geoHashValueMinMax = new long[2];
            geoHashValueMinMax[0] = r.prefix;
            geoHashValueMinMax[1] = (0xffffffffffffffffL>>>r.length)+r.prefix;
            if(this.sGeoHashLongs.empty()) {
                this.sGeoHashLongs.push(geoHashValueMinMax);
            }else{
                long[] geoHashValueMinMaxPop = this.sGeoHashLongs.pop();
                //相邻GeoHash段可以合并的条件：两个段组成的区域必须小于全球区域且相邻geoHash值相差为1
                if(geoHashValueMinMaxPop[0]!=0x8000000000000000L&&geoHashValueMinMaxPop[0]!=0xffffffffffffffffL
                        &&(geoHashValueMinMaxPop[0]-1)==geoHashValueMinMax[1]){
                    geoHashValueMinMax[1] = geoHashValueMinMaxPop[1];
                }else{
                    this.sGeoHashLongs.push(geoHashValueMinMaxPop);//若不合并，则将弹出的元素重新放到结果栈中
                }
                this.sGeoHashLongs.push(geoHashValueMinMax);
            }
        }
    }
    public double getAreaOfRectangleQueryScope(){
        DecimalFormat df = new DecimalFormat("#.00000");
        long deltaX = (long)(Double.parseDouble(df.format(Math.abs(rQS.deltaX)))*100000);
        long deltaY = (long)(Double.parseDouble(df.format(Math.abs(rQS.deltaY)))*100000);
        return (double)deltaX*deltaY;
    }
    public String toString(){
        return this.searchDepth+"#"+this.areaRatio+"#"+this.sizeOfGeoHashLongs;
    }

}
