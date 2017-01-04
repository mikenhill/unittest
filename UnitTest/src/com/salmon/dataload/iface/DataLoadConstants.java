package com.salmon.dataload.iface;

/**
 * This class holds a list of the possible error codes that can occur during the data load process.
 * 
 * @author Stephen Gair
 * @revision : 1.0
 * @Date : 19 December 2011
 */
public final class DataLoadConstants {

    /***********************************
     * Control-M recognises job exit codes as follows: 0 = Success 1-99 = Warning 100-255 = Error
     * 
     *** Please follow this convention ***
     * 
     ************************************/
    // internal errors
    public static final int INVALID_ARGUMENTS = 101;
    public static final int ERROR_WRITING_TARGET_FILE = 111;
    public static final int ERROR_WRITING_ARCHIVE_FILE = 112;
    public static final int ERROR_GENERATING_PRODUCT_ATTRIBUTES_FILE = 121;
    public static final int ERROR_GENERATING_SALES_CATALOG_FILE = 122;
    public static final int ERROR_DELETING_VALIDATED_FILE = 131;
    public static final int ERROR_DELETING_FILE = 132;

    // external errors - warning codes
    public static final int NO_SOURCE_FILE_FOUND = 11;
    public static final int EMPTY_FILE = 12;
    public static final int NO_RECORDS_TO_PROCESS = 13;
    public static final int INVALID_SUMMARY_FILE = 14;

    // external errors - fatal codes
    public static final int INVALID_RECORD_COUNT = 141;
    public static final int ERROR_READING_SOURCE_FILE = 142;
    public static final int TRAILER_IS_NOT_A_NUMBER = 143;
    public static final int INVALID_HEADER = 144;
    public static final int INVALID_FILE_COLUMN_COUNT = 145;
    public static final int INVALID_TRAILER = 146;

    // Customer Segment codes
    public static final String ERROR_HIGHER_THAN_THRESHOLD = "151";
    public static final String ERROR_INVALID_SEGMENT = "152";
    public static final String ERROR_SEGMENT_NOT_LEAF_NODE = "153";
    public static final String WARNING_THRESHOLD = "21";
    
    // EDW Usuals
    public static final String EDW_RESPONSE_DATA_FILE_HEADER_TYPE_STRING = "00";
    public static final String EDW_RESPONSE_DATA_FILE_DETAIL_TYPE_STRING = "10";
    public static final String EDW_RESPONSE_DATA_FILE_TRAILER_TYPE_STRING = "99";
    public static final int CSP_TRAILER_COUNT_START_INDEX = 2;
    public static final int CSP_TRAILER_COUNT_END_INDEX = 12;
    public static final String EDW_RESPONSE_CPS_FILE_NAME = "SB2Y_CDW_";
    public static final String EDW_RESPONSE_CPS_VALIDATED_FILE_NAME = "SB2Y_CDW_LOAD.dat";
    public static final String SYSTEM_EXIT_CODE_STRING = "Exit code : ";

    /**
     * private
     */
    private DataLoadConstants() {
    }
}
