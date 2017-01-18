package cc.xidian.MainTest;

import cc.xidian.GeoObject.GeoDataTableInfoA;
import cc.xidian.GeoObject.GeoIndexTableInfoA;
import cc.xidian.PhoenixOperation.PhoenixSQLOperation;
import cc.xidian.geoUtil.FileUtil;

import java.io.File;
import java.util.ArrayList;

/**
 * Created by hadoop on 2016/11/21.
 */
public class APIMainTest {
    public static void main(String[] args){
        PhoenixSQLOperation.getConnectionWithHBase();

//        PhoenixSQLOperation.getConnectionWithHBaseXenServerHDP();

        String strGeoDataTableNameA = "GEOPOINTTABLELWDSIMPLEUpsertTestNoBatch20MR8A";
        String strGeoDataTableNameB = "GEOPOINTTABLELWDSIMPLEUpsertTestNoBatch20MR8B";
//        String strGeoDataTableName = "GEOPOINTTABLELWDSIMPLESECONDINDEXTEST10MR12F";
        String[] strArrayGeoID = {"geoID","Integer","not null primary key"};
        String[] strArrayGeoName = {"geoName","varchar(32)"};
        String[] strArrayX = {"xLongitude","double"};
        String[] strArrayY = {"yLatitude","double"};
        String[] strArrayGeoHash = {"geoHashValueLong","bigint"};
        ArrayList<String[]> tableColumnNameAndTypeConstraint = new ArrayList<String[]>();
        tableColumnNameAndTypeConstraint.add(strArrayGeoID);
        tableColumnNameAndTypeConstraint.add(strArrayGeoName);
        tableColumnNameAndTypeConstraint.add(strArrayX);
        tableColumnNameAndTypeConstraint.add(strArrayY);
        tableColumnNameAndTypeConstraint.add(strArrayGeoHash);
        String strGeoDataTableConstraintA = "SALT_BUCKETS=35";
        String strGeoDataTableConstraintB = "IMMUTABLE_ROWS=true,SALT_BUCKETS=35";
        GeoDataTableInfoA geoDataTableInfoA = new GeoDataTableInfoA(strGeoDataTableNameA,tableColumnNameAndTypeConstraint,strGeoDataTableConstraintB);
        GeoDataTableInfoA geoDataTableInfoB = new GeoDataTableInfoA(strGeoDataTableNameB,tableColumnNameAndTypeConstraint,strGeoDataTableConstraintB);
        System.out.println(geoDataTableInfoA.getStrColumnSQL());

        String strGeoIndexTableNameA = "idx_geoHashValueLong_lwd_Covered_UpsertTestNoBatch20MR8A";
        String strGeoIndexTableNameB = "idx_geoHashValueLong_lwd_Covered_UpsertTestNoBatch20MR8B";
        ArrayList<String> indexColumnNames = new ArrayList<String>();
        indexColumnNames.add("geoHashValueLong");
        ArrayList<String> includeColumnNames = new ArrayList<String>();
        includeColumnNames.add("geoID");
        includeColumnNames.add("geoName");
        includeColumnNames.add("xLongitude");
        includeColumnNames.add("yLatitude");
        String strGeoIndexTableConstraintA = "SALT_BUCKETS=35";
        String strGeoIndexTableConstraintB = "ASYNC SALT_BUCKETS=35";
        GeoIndexTableInfoA geoIndexTableInfoA = new GeoIndexTableInfoA(strGeoIndexTableNameA,strGeoDataTableNameA,indexColumnNames,includeColumnNames,
                strGeoIndexTableConstraintA);
        GeoIndexTableInfoA geoIndexTableInfoB = new GeoIndexTableInfoA(strGeoIndexTableNameB,strGeoDataTableNameB,indexColumnNames,includeColumnNames,
                strGeoIndexTableConstraintA);

        PhoenixSQLOperation.dropGeoDataTable(geoDataTableInfoA);
//        PhoenixSQLOperation.dropGeoDataTable(geoDataTableInfoB);
        PhoenixSQLOperation.createGeoDateTableA(geoDataTableInfoA);
//        PhoenixSQLOperation.createGeoDateTableA(geoDataTableInfoB);
//        PhoenixSQLOperation.upsertGeoRecordsToGeoTable(geoDataTableInfoA);
//        PhoenixSQLOperation.dropGeoIndexTableA(geoIndexTableInfoA);
        PhoenixSQLOperation.createGeoIndexTableA(geoIndexTableInfoA);
//        PhoenixSQLOperation.createGeoIndexTableA(geoIndexTableInfoB);
//        PhoenixSQLOperation.upsertGeoRecordsToGeoTable(geoDataTableInfoA);

//        ArrayList<Long> upsertTimeArray = PhoenixSQLOperation.upsertGeoRecordsToGeoTableAndGetTime(geoDataTableInfoA);

        File fileUpsertTimeA = new File("upsertTimePer100k20MAACleanBatch2.txt");
//        File fileCommitTimeA = new File("lllcommitTimePer50K20MAB.txt");
//        File fileUpsertTimeB = new File("lllupsertTimeNoBatchPer5k20MAC.txt");
//        File fileCommitTimeB = new File("lllcommitTimeNoBatchPer5K20MAC.txt");
        PhoenixSQLOperation.upsertGeoRecordsToGeoTableAndGetTimeToFile(geoDataTableInfoA,fileUpsertTimeA);
//        PhoenixSQLOperation.upsertGeoRecordsToGeoTableANoBatchTimeToFile(geoDataTableInfoB,fileUpsertTimeB,fileCommitTimeB);
//        PhoenixSQLOperation.upsertGeoRecordsToGeoTable(geoDataTableInfoA);

//        File filePh = new File("upsertTimePer1k20MAB.txt");
//        File filePgBTree = new File("GeoPointTableLWDSimpleTestBatchP1K20MBTreeA.txt");
//        File filePgGist = new File("GeoPointTableLWDSimpleTestBatchP1K20MGISTA.txt");
//        File fileCombine = new File("InsertTestTimePPPBatch1k20MAA.txt");
//        FileUtil.getCombinedTimeFileFromPhoenixTimeFileAndPostgreSQLTimeFile(filePh,filePgBTree,filePgGist,fileCombine);

        PhoenixSQLOperation.closeConnectionWithHBase();

    }
}
