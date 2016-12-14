package cc.xidian.GeoObject;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by hadoop on 2016/11/21.
 */
public class GeoDataTableInfoA {
    private String strGeoDataTableName;//表名
    //private HashMap<String,String> tableColumnNameAndTypeConstraint;//表的列名和类型约束
    private ArrayList<String[]> tableColumnNameAndTypeConstraint;
    private String strGeoDataTableConstraint;//表的约束
    private int numOfTableRecords;//表中记录的条数

    public GeoDataTableInfoA(){
        this.strGeoDataTableName = "";
        this.tableColumnNameAndTypeConstraint = new ArrayList<String[]>();
        this.strGeoDataTableConstraint = "";
        this.numOfTableRecords = 0;
    }

    public GeoDataTableInfoA(String strGeoDataTableName, ArrayList<String[]> tableColumnNameAndTypeConstraint, String strGeoDataTableConstraint, int numOfTableRecords) {
        this.strGeoDataTableName = strGeoDataTableName;
        this.tableColumnNameAndTypeConstraint = tableColumnNameAndTypeConstraint;
        this.strGeoDataTableConstraint = strGeoDataTableConstraint;
        this.numOfTableRecords = numOfTableRecords;
    }

    public GeoDataTableInfoA(String strGeoTableName, ArrayList<String[]> tableColumnNameAndTypeConstraint, String strTableConstraint){
        this.strGeoDataTableName = strGeoTableName;
        this.tableColumnNameAndTypeConstraint = tableColumnNameAndTypeConstraint;
        this.strGeoDataTableConstraint = strTableConstraint;
    }

    public String getStrGeoDataTableName() {
        return strGeoDataTableName;
    }

    public void setStrGeoDataTableName(String strGeoDataTableName) {
        this.strGeoDataTableName = strGeoDataTableName;
    }

    public ArrayList<String[]> getTableColumnNameAndTypeConstraint() {
        return tableColumnNameAndTypeConstraint;
    }

    public void setTableColumnNameAndTypeConstraint(ArrayList<String[]> tableColumnNameAndTypeConstraint) {
        this.tableColumnNameAndTypeConstraint = tableColumnNameAndTypeConstraint;
    }

    public int getNumOfTableRecords() {
        return numOfTableRecords;
    }

    public void setNumOfTableRecords(int numOfTableRecords) {
        this.numOfTableRecords = numOfTableRecords;
    }

    public String getStrGeoDataTableConstraint() {
        return strGeoDataTableConstraint;
    }

    public void setStrGeoDataTableConstraint(String strGeoDataTableConstraint) {
        this.strGeoDataTableConstraint = strGeoDataTableConstraint;
    }

    @Override
    public String toString() {
        return "GeoDataTableInfoA{" +
                "strGeoDataTableName='" + strGeoDataTableName + '\'' +
                ", tableColumnNameAndTypeConstraint=" + tableColumnNameAndTypeConstraint +
                ", strGeoDataTableConstraint='" + strGeoDataTableConstraint + '\'' +
                ", numOfTableRecords=" + numOfTableRecords +
                '}';
    }

    public String getStrColumnSQL(){
        String strColumnSQL = " ( ";
        int sizeOfColumns = this.tableColumnNameAndTypeConstraint.size();
        if(sizeOfColumns>1) {
            for (int i = 0; i < sizeOfColumns - 1; i++) {
                String[] strArray = this.tableColumnNameAndTypeConstraint.get(i);
                for (int j = 0; j < strArray.length - 1; j++) {
                    strColumnSQL += strArray[j] + "    ";
                }
                strColumnSQL += strArray[strArray.length - 1] + ",";
            }
        }
        String[] strArray = this.tableColumnNameAndTypeConstraint.get(sizeOfColumns-1);
        for(int j=0;j<strArray.length-1;j++){
            strColumnSQL+=strArray[j]+"    ";
        }
        strColumnSQL+=strArray[strArray.length-1]+" ) ";

        return strColumnSQL;
    }
    public String getStrUpsertInterrogationSQL(){
        String strUpsertInterrogationSQL = " ( ";
        int sizeOfColumns = this.tableColumnNameAndTypeConstraint.size();
        if (sizeOfColumns>1) {
            for (int i = 0; i < sizeOfColumns - 1; i++) {
                strUpsertInterrogationSQL += "?,";
            }
        }
        strUpsertInterrogationSQL+="? ) ";

        return strUpsertInterrogationSQL;
    }
}
