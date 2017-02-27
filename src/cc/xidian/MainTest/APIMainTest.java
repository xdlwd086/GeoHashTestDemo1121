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
//        PhoenixSQLOperation.getConnectionWithHBase();

        PhoenixSQLOperation.getConnectionWithHBaseXenServerHDP();

        String strGeoDataTableNameA = "GEOPOINTTABLELWDSIMPLEBulkLoader100MR20A";
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
        String strGeoDataTableConstraintB = "IMMUTABLE_ROWS=true,SALT_BUCKETS=20";
        GeoDataTableInfoA geoDataTableInfoA = new GeoDataTableInfoA(strGeoDataTableNameA,tableColumnNameAndTypeConstraint,strGeoDataTableConstraintB);
        GeoDataTableInfoA geoDataTableInfoB = new GeoDataTableInfoA(strGeoDataTableNameB,tableColumnNameAndTypeConstraint,strGeoDataTableConstraintB);
        System.out.println(geoDataTableInfoA.getStrColumnSQL());

        String strGeoIndexTableNameA = "idx_geoHashValueLong_lwd_Covered_BulkLoader100MR20A";
        String strGeoIndexTableNameB = "idx_geoHashValueLong_lwd_Covered_UpsertTestNoBatch20MR8B";
        ArrayList<String> indexColumnNames = new ArrayList<String>();
        indexColumnNames.add("geoHashValueLong");
        ArrayList<String> includeColumnNames = new ArrayList<String>();
        includeColumnNames.add("geoID");
        includeColumnNames.add("geoName");
        includeColumnNames.add("xLongitude");
        includeColumnNames.add("yLatitude");
        String strGeoIndexTableConstraintA = "SALT_BUCKETS=20";
        String strGeoIndexTableConstraintB = "ASYNC SALT_BUCKETS=20";
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

//        File fileUpsertTimeA = new File("upsertTimePer100k20MAACleanBatch2.txt");
//        File fileCommitTimeA = new File("lllcommitTimePer50K20MAB.txt");
//        File fileUpsertTimeB = new File("lllupsertTimeNoBatchPer5k20MAC.txt");
//        File fileCommitTimeB = new File("lllcommitTimeNoBatchPer5K20MAC.txt");
//        PhoenixSQLOperation.upsertGeoRecordsToGeoTableAndGetTimeToFile(geoDataTableInfoA,fileUpsertTimeA);
//        PhoenixSQLOperation.upsertGeoRecordsToGeoTableANoBatchTimeToFile(geoDataTableInfoB,fileUpsertTimeB,fileCommitTimeB);
//        PhoenixSQLOperation.upsertGeoRecordsToGeoTable(geoDataTableInfoA);

//        File filePh = new File("upsertTimePer1k20MAB.txt");
//        File filePgBTree = new File("GeoPointTableLWDSimpleTestBatchP1K20MBTreeA.txt");
//        File filePgGist = new File("GeoPointTableLWDSimpleTestBatchP1K20MGISTA.txt");
//        File fileCombine = new File("InsertTestTimePPPBatch1k20MAA.txt");
//        FileUtil.getCombinedTimeFileFromPhoenixTimeFileAndPostgreSQLTimeFile(filePh,filePgBTree,filePgGist,fileCombine);


//        String strGeoDataTableNameGDELTXYT = "GDELTLWD2013R35GEOHASHXYTA";
//        String strGeoDataTableNameGDELT = "GDELTLWD201612R20GEOHASHA";
//        String[] strGlobalEventID = {"globalEventID","Integer","not null primary key"};
//        String[] strSQLData = {"sqlDate","Integer"};
//        String[] strActor1Code = {"actor1Code","varchar"};
//        String[] strActor1Name = {"actor1Name","varchar"};
//        String[] strActor1CountryCode = {"actor1CountryCode","varchar"};
//        String[] strActionGeo_Type = {"actionGeo_Type","Integer"};
//        String[] strActionGeo_CountryCode = {"actionGeo_CountryCode","varchar"};
//        String[] strActionGeo_ADM1Code = {"actionGeo_ADM1Code","varchar"};
//        String[] strActionGeo_Long = {"actionGeo_Long","double"};
//        String[] strActionGeo_Lat = {"actionGeo_Lat","double"};
//        String[] strActionGeo_GeoHashValue = {"actionGeo_GeoHashValue","bigint"};
//        String[] strActionGeo_FeatureID = {"actionGeo_FeatureID","varchar"};
//        String[] strDateAdded = {"dateAdded","Integer"};
//        String[] strSourceURL = {"sourceURL","varchar"};
//        ArrayList<String[]> tableColumnNameAndTypeConstraintGDELT = new ArrayList<String[]>();
//        tableColumnNameAndTypeConstraintGDELT.add(strGlobalEventID);
//        tableColumnNameAndTypeConstraintGDELT.add(strSQLData);
//        tableColumnNameAndTypeConstraintGDELT.add(strActor1Code);
//        tableColumnNameAndTypeConstraintGDELT.add(strActor1Name);
//        tableColumnNameAndTypeConstraintGDELT.add(strActor1CountryCode);
//        tableColumnNameAndTypeConstraintGDELT.add(strActionGeo_Type);
//        tableColumnNameAndTypeConstraintGDELT.add(strActionGeo_CountryCode);
//        tableColumnNameAndTypeConstraintGDELT.add(strActionGeo_ADM1Code);
//        tableColumnNameAndTypeConstraintGDELT.add(strActionGeo_Long);
//        tableColumnNameAndTypeConstraintGDELT.add(strActionGeo_Lat);
//        tableColumnNameAndTypeConstraintGDELT.add(strActionGeo_GeoHashValue);
//        tableColumnNameAndTypeConstraintGDELT.add(strActionGeo_FeatureID);
//        tableColumnNameAndTypeConstraintGDELT.add(strDateAdded);
//        tableColumnNameAndTypeConstraintGDELT.add(strSourceURL);
//        String strGeoDataTableConstraintGDELT = "IMMUTABLE_ROWS=true,SALT_BUCKETS=20";
//        GeoDataTableInfoA geoDataTableInfoAGDELT = new GeoDataTableInfoA(strGeoDataTableNameGDELT,
//                tableColumnNameAndTypeConstraintGDELT,strGeoDataTableConstraintGDELT);
//
//        String strGeoIndexTableInfoNameGDELT = "idx_GeoHash_LWD_Covered_GDELTEVENT201612R20";
//        ArrayList<String> indexColumnNamesGDELT = new ArrayList<String>();
//        indexColumnNamesGDELT.add("actionGeo_GeoHashValue");
////        indexColumnNamesGDELT.add("actionGeo_Long");
////        indexColumnNamesGDELT.add("actionGeo_Lat");
////        indexColumnNamesGDELT.add("sqlDate");
//        ArrayList<String> includeColumnNamesGDELT = new ArrayList<String>();
//        includeColumnNamesGDELT.add("globalEventID");
//        includeColumnNamesGDELT.add("sqlDate");
//        includeColumnNamesGDELT.add("actor1Code");
//        includeColumnNamesGDELT.add("actor1Name");
//        includeColumnNamesGDELT.add("actor1CountryCode");
//        includeColumnNamesGDELT.add("actionGeo_Type");
//        includeColumnNamesGDELT.add("actionGeo_CountryCode");
//        includeColumnNamesGDELT.add("actionGeo_ADM1Code");
//        includeColumnNamesGDELT.add("actionGeo_Long");
//        includeColumnNamesGDELT.add("actionGeo_Lat");
//        includeColumnNamesGDELT.add("actionGeo_FeatureID");
//        includeColumnNamesGDELT.add("dateAdded");
//        includeColumnNamesGDELT.add("sourceURL");
//        String strGeoIndexTableConstraintGDELT = "SALT_BUCKETS=20";
//        GeoIndexTableInfoA geoIndexTableInfoAGDELT = new GeoIndexTableInfoA(strGeoIndexTableInfoNameGDELT,
//                strGeoDataTableNameGDELT,indexColumnNamesGDELT,includeColumnNamesGDELT,strGeoIndexTableConstraintGDELT);
//
//        PhoenixSQLOperation.dropGeoDataTable(geoDataTableInfoAGDELT);
//        PhoenixSQLOperation.createGeoDateTableA(geoDataTableInfoAGDELT);
//        PhoenixSQLOperation.createGeoIndexTableA(geoIndexTableInfoAGDELT);


        PhoenixSQLOperation.closeConnectionWithHBase();

    }
}
