package com.salmon.dataload.iface;

import static com.salmon.dataload.iface.DataLoadConstants.EMPTY_FILE;
import static com.salmon.dataload.iface.DataLoadConstants.ERROR_READING_SOURCE_FILE;
import static com.salmon.dataload.iface.DataLoadConstants.INVALID_ARGUMENTS;
import static com.salmon.dataload.iface.DataLoadConstants.INVALID_HEADER;
import static com.salmon.dataload.iface.DataLoadConstants.NO_SOURCE_FILE_FOUND;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;

import com.salmon.dataload.helper.MonitiseEtlHelper;
import com.salmon.dataload.helper.TableName;
import com.salmon.dataload.utils.FileUtilities;
import com.salmon.dataload.utils.UnicodeBOMInputStream;

/**
 * This class scans the sourceFilePath for a file with the given sourceFileName. 
 * 
 * @author Pranava Mishra
 * @revision : 1.0
 * @Date : 25 May 2014
 */
public final class ValidateAttributeCSVDataFile {

    static final String CSV = ".csv";
    static final String TRAILER_DELIMITER = "\\|";
    static final String COLUMN_DELIMITER = "|";
    static final String DELIMITER = "\\|";
    static final String ATTRIBUTE_DELIMITER = "\\,";
    static final String COLUMN_FILENAME = "filename";

    private static final String CLASSNAME = ValidateAttributeCSVDataFile.class.getName();
    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(CLASSNAME);    

    private static final int NUM_ARGUMENTS       = 8;
    private static final int SOURCE_FILE_PATH    = 0;
    private static final int SOURCE_FILE_NAME    = 1;
    private static final int VALIDATED_FILE_NAME = 2;
    private static final int EXPECTED_HEADER     = 3;
    private static final int JDBC_DRIVER         = 4;
    private static final int DB_URL              = 5;
    private static final int DB_USERNAME         = 6;
    private static final int DB_PASSWORD         = 7;
    
    private static final int CPCODE         = 0;
    private static final int PRODUCTTYPE    = 1;
    private static final int CPSKU          = 2;   
    private static final int ATTRIBUTENAME  = 3;
    private static final int ATTRIBUTEVALUE = 4;
    private static final int SEQUENCENO     = 5;
    
    private static final int ATTRNAME          = 0;
    private static final int ATTRIBUTEPROPERTY = 1;
    private static final int ATTRIBUTETYPE     = 2;
    
    private final String sourceFilePath;
    private final String sourceFileName;
    private final String validatedFileName;
    private final String expectedHeader;
    
    /**
     * Constructor
     * 
     * @param sourceFilePath - source file path
     * @param sourceFilePrefix - source file prefix
     * @param validatedFileName - validated file name
     * @param expectedHeader - the expected header
     */
    public ValidateAttributeCSVDataFile(final String sourceFilePath, final String sourceFileName, final String validatedFileName, final String expectedHeader) {
        this.sourceFilePath    = sourceFilePath;
        this.sourceFileName    = sourceFileName;
        this.validatedFileName = validatedFileName;
        this.expectedHeader    = expectedHeader;
    }

    /**
     * Three arguments must be passed representing the source file path, 
     * the source file prefix and the validated file name.
     * 
     * @param args - input parameters
     */
    public static void main(final String[] args) {        
        String methodName = "main";
        if (args.length == NUM_ARGUMENTS && args[SOURCE_FILE_PATH].length() > 0 && args[SOURCE_FILE_NAME].length() > 0
            && args[VALIDATED_FILE_NAME].length() > 0 && args[EXPECTED_HEADER].length() > 0
            && args[JDBC_DRIVER].length() > 0 && args[DB_URL].length() > 0 && args[DB_USERNAME].length() > 0 
            && args[DB_PASSWORD].length() > 0 ) 
        {
            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Validating file: " + args[SOURCE_FILE_PATH]);            
            
            ValidateAttributeCSVDataFile validate = new ValidateAttributeCSVDataFile(args[SOURCE_FILE_PATH], 
                                                                                     args[SOURCE_FILE_NAME], 
                                                                                     args[VALIDATED_FILE_NAME],
                                                                                     args[EXPECTED_HEADER]);
            validate.run(args[JDBC_DRIVER], args[DB_URL], args[DB_USERNAME], args[DB_PASSWORD]);
            
        } else {
            LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "Invalid arguments passed to ValidateStockCSVDataFile");
            System.exit(INVALID_ARGUMENTS);
        }
    }


    /**
     * Read the input source file. If the trailer count matches the number of records, 
     * then write the validated file and archive the source file.
     */
    private void run(String jdbcDriver, String dbURL, String dbUserName, String dbPassword ) {      
        String methodName = "run";
        LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Validating data of CSV file: ");
        
        File sourceFile = new File(sourceFilePath, validatedFileName);
    
        long totalLinesCount = FileUtilities.getTotalLinesCount(sourceFile);
        if (totalLinesCount == 0) {
            LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, sourceFileName + " is empty");            
            System.exit(EMPTY_FILE);
        }
        
        String header = readHeader(sourceFile);
        boolean validHeader = validateHeader(header);        
        if (!validHeader) {
            LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "The header " + header + " is not valid, expecting: " + expectedHeader);
            System.exit(INVALID_HEADER);
        }
        
        try{
            MonitiseEtlHelper monitiseEtlHelper = new MonitiseEtlHelper(jdbcDriver, dbURL, dbUserName, dbPassword);
            validateCSVData(monitiseEtlHelper, sourceFile, totalLinesCount);
            monitiseEtlHelper.commit();
            monitiseEtlHelper.close();            
        } catch(SQLException sql){
            LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "SQLException Exception"+sql.getMessage());
            logStackTrace(sql, "run");
            System.exit(NO_SOURCE_FILE_FOUND);        
        } catch(ClassNotFoundException cnfe){
            LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "ClassNotFoundException Exception"+cnfe.getMessage());
            logStackTrace(cnfe, "run");
            System.exit(NO_SOURCE_FILE_FOUND);        
        }      
    }
    
    
    /**
     * Check that the header record matches the expected header record.
     * 
     * @param header - the header
     * @return - indicates if the header is valid or not.
     */
    private boolean validateHeader(final String header) {
        return expectedHeader.equalsIgnoreCase(header.trim());
    }

    /**
     * Read and return the source file header
     * 
     * @param sourceFile - the file to be read.
     * @return the header
     */
    private static String readHeader(final File sourceFile) {
        LOGGER.entering(CLASSNAME, "readHeader");
        String strLine = "";
        try {
            FileInputStream fis = new FileInputStream(sourceFile);
            InputStream cleanStream = new UnicodeBOMInputStream(fis).skipBOM();
            InputStreamReader in = new InputStreamReader(cleanStream, "UTF-8");
            BufferedReader br = new BufferedReader(in);
            strLine = br.readLine();
            in.close();
        } catch (Exception e) {
            logStackTrace(e, "readHeader");
            System.exit(ERROR_READING_SOURCE_FILE);
        }
        LOGGER.exiting(CLASSNAME, "readHeader");
        return strLine;
    }


    /**
     * Write the validated output file data (which is the same as the input file without the trailer and 
     * with append the source file name).      
     * @param sourceFile - the source file
     * @param lineCount - the number of lines in the source file
     */
    public void validateCSVData(MonitiseEtlHelper  monitiseEtlHelper, File sourceFile, final long lineCount) throws SQLException {
        String methodName = "validateCSVData";
        LOGGER.entering(CLASSNAME, methodName, validatedFileName);
        try {
            
            InputStream fis     = getFileInputStream(sourceFile);
            InputStream cleanStream = new UnicodeBOMInputStream(fis).skipBOM();
            InputStreamReader in    = new InputStreamReader(cleanStream, "UTF-8");
            BufferedReader br       = new BufferedReader(in);

            HashMap<String, String> mapPT        = new HashMap<String, String>();
            HashMap<String, String> mapPTValCond = new HashMap<String, String>(); // M=Mandatory, O=Optional
            HashMap<String, String> mapPTValType = new HashMap<String, String>(); // N=Numeric, D=Date, L=List, X=Free format, I=Integer
            ArrayList<String> mandatoryAttribtues = new ArrayList<String>();

            
            String currSeqNo        = "Initial";
            String offerStatus      = "0";
            String missingAttribute = null;
            
            for (int i = 0; i < lineCount; i++) {
                
                String strLine = br.readLine();  
                
                if(i>0)
                {
                    String[] dataArray=strLine.split(DELIMITER);
                    String eCode=""; 
                    missingAttribute = null;
                    int index=0;
                    
                    if (!currSeqNo.equals(dataArray[SEQUENCENO])) {
                        LOGGER.logp(Level.INFO, CLASSNAME, methodName,  " Offer Sequence number:" + dataArray[SEQUENCENO]);
                        currSeqNo = dataArray[SEQUENCENO];
                        offerStatus = monitiseEtlHelper.getOfferStatus(currSeqNo);
                    }
                   
                    for(String data : dataArray){
                        
                        switch(index){
                        case CPCODE: 
                            // No validation required as validation is done by offers load
                            break;
                        case CPSKU:
                         // No validation required as validation is done by offers load 
                            break;
                        case PRODUCTTYPE:
                            LOGGER.logp(Level.INFO, CLASSNAME, methodName,  data + " is productType");
                            // No validation just retrieve Cached entry
                            if (!mapPT.containsKey(data)) {
                                String allowedAttributes=monitiseEtlHelper.getAllowedAttributes(data);                                    
                                if (allowedAttributes != null) {
                                    mapPT.put(data, allowedAttributes);
                                
                                    // code to check if all mandatory attributes are supplied for the product//                                
                                    
                                    String[] attributeArray=allowedAttributes.split(DELIMITER);
                                    
                                    for(int j=0, max = attributeArray.length; j < max; j++) {                                    
                                       String[] attribute = attributeArray[j].split(ATTRIBUTE_DELIMITER);
                                       if(attribute[ATTRIBUTEPROPERTY].equals("M")){
                                          mandatoryAttribtues.add(attribute[ATTRNAME]); 
                                            LOGGER.logp(Level.INFO, CLASSNAME, methodName, attribute[ATTRNAME] + " is added to list");
                                        }
                                     }
                                }
                                
                                String attributes=monitiseEtlHelper.getAllAttributes(dataArray[SEQUENCENO]);
                                if (attributes != null && mandatoryAttribtues.size() > 0) {
                                    String[] attributeList=attributes.split(DELIMITER);
                                    for(int k=0, mx = attributeList.length; k < mx; k++) { 
                                        LOGGER.logp(Level.INFO, CLASSNAME, methodName, mandatoryAttribtues.indexOf(attributeList[k].trim()) + " is index");
                                        if(mandatoryAttribtues.contains(attributeList[k])){
                                            mandatoryAttribtues.remove(attributeList[k]);
                                            LOGGER.logp(Level.INFO, CLASSNAME, methodName, attributeList[k] + " is removed");
                                      }
                                    }
                                    for(int l=0; l< mandatoryAttribtues.size(); l++){
                                       missingAttribute=(missingAttribute==null?mandatoryAttribtues.get(l):missingAttribute+COLUMN_DELIMITER+mandatoryAttribtues.get(l));
                                    }
                                    LOGGER.logp(Level.INFO, CLASSNAME, methodName, "missing mandatory attribtues:" + mandatoryAttribtues);
                                }
                                mandatoryAttribtues.clear();
                            }
                            break;
                        case ATTRIBUTENAME:
                            LOGGER.logp(Level.INFO, CLASSNAME, methodName, data + " is attributeName");
                            
                            if (!mapPTValCond.containsKey(dataArray[PRODUCTTYPE] + data)) {
                                String allowedAttributes = mapPT.get(dataArray[PRODUCTTYPE]);
                                if (allowedAttributes != null) {
                                    String[] attributeArray=allowedAttributes.split(DELIMITER);
                                    for(int j=0, max = attributeArray.length; j < max; j++) {                                    
                                        String[] attributeProperty = attributeArray[j].split(ATTRIBUTE_DELIMITER);
                                        
                                        mapPTValCond.put(dataArray[PRODUCTTYPE] + attributeProperty[ATTRNAME] , attributeProperty[ATTRIBUTEPROPERTY]);
                                        mapPTValType.put(dataArray[PRODUCTTYPE] + attributeProperty[ATTRNAME] , attributeProperty[ATTRIBUTETYPE]);
                                        
                                    }       
                                    
                                    if (!mapPTValCond.containsKey(dataArray[PRODUCTTYPE] + dataArray[ATTRIBUTENAME])) {
                                        if (eCode!=null && eCode.length() > 0){ 
                                            eCode = eCode + COLUMN_DELIMITER + "E019";
                                        } else {
                                            eCode = "E019";
                                        }
                                    }
                                }
                            }
                            break;
                        case ATTRIBUTEVALUE:
                            LOGGER.logp(Level.INFO, CLASSNAME, methodName, data + " is attributeValue");
                            
                            String allowedAttributes = mapPT.get(dataArray[PRODUCTTYPE]);   // eg: giftCard should return.......                                                     
                            // giftCardValue,M,N|giftcardExpiryDate,O,D|purchaseFee,O,N|secureShippingFee,O,N|shipToConsumerMethod,M,L|redemptionMethod,O,L
                            LOGGER.logp(Level.INFO, CLASSNAME, methodName,  "ProductTypeFields:" + allowedAttributes);
                            
                            String valCond = mapPTValCond.get(dataArray[PRODUCTTYPE] + dataArray[ATTRIBUTENAME]);
                            String valType = mapPTValType.get(dataArray[PRODUCTTYPE] + dataArray[ATTRIBUTENAME]);
                            
                            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "valCond:" + valCond + " valType:" + valType);
                            
                            if (data == null || data.trim().length() == 0) {  
                                
                                if ("M".equals(valCond)) {  // Mandatory attribute                                                               
                                    eCode = "E020:"+dataArray[ATTRIBUTENAME];
                                }                   
                                
                            } else {
                            
                                if ("I".equals(valType)){       // Integer                                    
                                    eCode=monitiseEtlHelper.isValidInteger(dataArray[ATTRIBUTEVALUE], dataArray[ATTRIBUTENAME]);
                                }
                                else if ("N".equals(valType)){  // Numeric 
                                    eCode=monitiseEtlHelper.isValidNumericValue(dataArray[ATTRIBUTEVALUE], dataArray[ATTRIBUTENAME]);
                                }
                                else if ("D".equals(valType)){  // Date 
                                    eCode=monitiseEtlHelper.isValidAttributeDate(dataArray[ATTRIBUTEVALUE], dataArray[ATTRIBUTENAME]);
                                }
                                else if ("L".equals(valType)){  // List 
                                    
                                    if (dataArray[ATTRIBUTENAME].indexOf("SubCategory") > 0) {  
                                        
                                        // Need to extract Category selection from xint_attributedata and use that value appended
                                        // to category attribute name to see if sub category selection is valid (within input file name).                                        
                                        String subCategoryName   = dataArray[ATTRIBUTENAME];
                                        String categoryName      = subCategoryName.replace("Sub", "");
                                        LOGGER.logp(Level.INFO, CLASSNAME, methodName, "subCategoryName:" + subCategoryName + " categoryName:" + categoryName);
                                        String categorySelection = monitiseEtlHelper.getCategorySelection(categoryName, dataArray[SEQUENCENO]);
                                        
                                        categoryName = categoryName + categorySelection;
                                        
                                        eCode=monitiseEtlHelper.isValidPreDefinedValue(data, categoryName);
                                        
                                    } else {
                                        
                                        eCode=monitiseEtlHelper.isValidPreDefinedValue(dataArray[ATTRIBUTEVALUE],dataArray[ATTRIBUTENAME]);
                                    
                                    }
                                }
                            }
                            break;
                        }
                        index++;
                    }
                    
                    String status=(eCode != null && eCode.length() > 0)?"2":"1";
                    if (eCode == null) {
                        eCode="";
                    }
                    
                    if (missingAttribute != null) {  // Missing Mandatory Attribute
                        eCode = "E020:" + missingAttribute;
                        status = "2";
                    }
                    
                    if ("2".equals(offerStatus)) {  // Data is invalid on associated Offer therefore all attributes need to be set to error
                        status = "2";
                    }            
                    
                    LOGGER.logp(Level.INFO, CLASSNAME, methodName, "updateErrorCodeBySeq currSeqNo:" + currSeqNo + " eCode:" + eCode + " status:" + status + " offerStatus:" + offerStatus);
                    String processed=monitiseEtlHelper.getProcessedFlag(currSeqNo,dataArray[ATTRIBUTENAME]).trim();
                    monitiseEtlHelper.updateErrorCodeBySeq(TableName.XINT_ATTRIBUTEDATA, dataArray[ATTRIBUTENAME], currSeqNo, eCode, status);
                    
                    if ("2".equals(status) || "2".equals(processed) ) {  // Errors have been found                        
                        LOGGER.logp(Level.INFO, CLASSNAME, methodName, "setAllAttributesToFailed currSeqNo:" + currSeqNo);
                        monitiseEtlHelper.setAllAttributesToFailed(currSeqNo);
                        
                        if (!("2".equals(offerStatus))) {

                            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "setOfferToFailed:" + currSeqNo);
                            
                            monitiseEtlHelper.setOfferToFailed(currSeqNo);
                            
                        }
                    }
                }
            }
            
            br.close();
            in.close();
        } catch (SQLException sqle) {
            logStackTrace(sqle, methodName);                      
        } catch (Exception e) {
            logStackTrace(e, methodName);                        
        }finally{
            monitiseEtlHelper.commit();
            monitiseEtlHelper.close();
        }
        LOGGER.exiting(CLASSNAME, methodName);

    }
    
    private InputStream getFileInputStream(File sourceFile) throws FileNotFoundException {
        return new FileInputStream(sourceFile);
    }


    /**
     * Log the Stack Trace
     * 
     * @param aThrowable - the throwable.
     * @param methodName - the method name.
     */
    private static void logStackTrace(final Throwable aThrowable, final String methodName) {
        Writer result = new StringWriter();
        aThrowable.printStackTrace(new PrintWriter(result));
        LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, result.toString());
    }
}