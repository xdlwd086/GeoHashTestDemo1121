package cc.xidian.GeoObject;

import java.util.ArrayList;

/**
 * Created by hadoop on 2016/11/21.
 */
public class GeoIndexTableInfoA {
    private String strGeoIndexTableName;
    private String strGeoDataTableName;
    private ArrayList<String> indexColumnNames;
    private ArrayList<String> includeColumnNames;
    private String strGeoIndexTableConstraint;

    public GeoIndexTableInfoA() {
        this.strGeoIndexTableName = "";
        this.indexColumnNames = new ArrayList<String>();
        this.strGeoIndexTableConstraint = "";
    }

    public GeoIndexTableInfoA(String strGeoIndexTableName, String strGeoDataTableName, ArrayList<String> indexColumnNames,
                              ArrayList<String> includeColumnNames, String strGeoIndexTableConstraint) {
        this.strGeoIndexTableName = strGeoIndexTableName;
        this.strGeoDataTableName = strGeoDataTableName;
        this.indexColumnNames = indexColumnNames;
        this.includeColumnNames = includeColumnNames;
        this.strGeoIndexTableConstraint = strGeoIndexTableConstraint;
    }

    public String getStrGeoIndexTableName() {
        return strGeoIndexTableName;
    }

    public void setStrGeoIndexTableName(String strGeoIndexTableName) {
        this.strGeoIndexTableName = strGeoIndexTableName;
    }

    public ArrayList<String> getIndexColumnNames() {
        return indexColumnNames;
    }

    public void setIndexColumnNames(ArrayList<String> indexColumnNames) {
        this.indexColumnNames = indexColumnNames;
    }

    public String getStrGeoIndexTableConstraint() {
        return strGeoIndexTableConstraint;
    }

    public void setStrGeoIndexTableConstraint(String strGeoIndexTableConstraint) {
        this.strGeoIndexTableConstraint = strGeoIndexTableConstraint;
    }

    public String getStrGeoDataTableName() {
        return strGeoDataTableName;
    }

    public void setStrGeoDataTableName(String strGeoDataTableName) {
        this.strGeoDataTableName = strGeoDataTableName;
    }

    public ArrayList<String> getIncludeColumnNames() {
        return includeColumnNames;
    }

    public void setIncludeColumnNames(ArrayList<String> includeColumnNames) {
        this.includeColumnNames = includeColumnNames;
    }

    @Override
    public String toString() {
        return "GeoIndexTableInfoA{" +
                "strGeoIndexTableName='" + strGeoIndexTableName + '\'' +
                ", indexColumnNames=" + indexColumnNames +
                ", strGeoIndexTableConstraint='" + strGeoIndexTableConstraint + '\'' +
                '}';
    }

    public String getStrIndexColumnNames(){
        String strIndexColumnNames = "(";
        if(this.indexColumnNames.size()>1){
            for(int i=0;i<this.indexColumnNames.size()-1;i++) {
                strIndexColumnNames += this.indexColumnNames.get(i) + ",";
            }
        }
        strIndexColumnNames+=this.indexColumnNames.get(this.indexColumnNames.size()-1)+")";
        return strIndexColumnNames;
    }

    public String getStrIncludeColumnNames(){
        String strIncludeColumnNames = "(";
        if(this.includeColumnNames.size()>1){
            for(int i=0;i<this.includeColumnNames.size()-1;i++) {
                strIncludeColumnNames += this.includeColumnNames.get(i) + ",";
            }
        }
        strIncludeColumnNames+=this.includeColumnNames.get(this.includeColumnNames.size()-1)+")";
        return strIncludeColumnNames;
    }
}
