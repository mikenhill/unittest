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
public final class ValidateProductCSVDataFile {

    static final String CSV = ".csv";
    static final String TRAILER_DELIMITER = "\\|";
    static final String COLUMN_DELIMITER = "|";
    static final String DELIMITER = "\\|";
    static final String COLUMN_FILENAME = "filename";

    private static final String CLASSNAME = ValidateProductCSVDataFile.class.getName();
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
    private static final int CPPARENTSKU    = 3;
    private static final int LANGUAGE       = 4;
    private static final int NAME           = 5;
    private static final int SHORTDESC      = 6;
    private static final int LISTPRICE      = 8;
    private static final int OFFERPRICE     = 9;
    private static final int CURRENCY       = 10;
    private static final int ONSPECIAL      = 11;
    private static final int PHYSICALSTOCK  = 12;
    private static final int INVENTORY      = 13;
    private static final int IMAGE1         = 14;
    private static final int IMAGE2         = 15;
    private static final int IMAGE3         = 16;
    private static final int IMAGE4         = 17;
    private static final int STARTDATE      = 18;
    private static final int ENDDATE        = 19;
    private static final int AGERESTRICTION = 20;
    private static final int KEYWORD        = 25;
    private static final int SEQUENCE_NO    = 26;
    
    private static final String LISTPRICECOLNAME  = "list price";
    private static final String OFFERPRICECOLNAME = "offer price";
    
    
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
    public ValidateProductCSVDataFile(final String sourceFilePath, final String sourceFileName, final String validatedFileName, final String expectedHeader) {
        this.sourceFilePath = sourceFilePath;
        this.sourceFileName = sourceFileName;
        this.validatedFileName = validatedFileName;
        this.expectedHeader = expectedHeader;
    }

    /**
     * Three arguments must be passed representing the source file path, the source file prefex and the validated file name.
     * 
     * @param args - input parameters
     */
    public static void main(final String[] args) {        
        if (args.length == NUM_ARGUMENTS && args[SOURCE_FILE_PATH].length() > 0 && args[SOURCE_FILE_NAME].length() > 0
            && args[VALIDATED_FILE_NAME].length() > 0 && args[EXPECTED_HEADER].length() > 0
            && args[JDBC_DRIVER].length() > 0 && args[DB_URL].length() > 0 && args[DB_USERNAME].length() > 0 
            && args[DB_PASSWORD].length() > 0 
        ) {
            LOGGER.logp(Level.INFO, CLASSNAME, "main", "Validating file: " + args[SOURCE_FILE_PATH]);            
            ValidateProductCSVDataFile validate = new ValidateProductCSVDataFile(args[SOURCE_FILE_PATH], args[SOURCE_FILE_NAME], args[VALIDATED_FILE_NAME],
                    args[EXPECTED_HEADER]);
            validate.run(args[JDBC_DRIVER], args[DB_URL], args[DB_USERNAME], args[DB_PASSWORD]);
            
        } else {
            LOGGER.logp(Level.SEVERE, CLASSNAME, "main", "Invalid arguments passed to ValidateStockCSVDataFile");
            System.exit(INVALID_ARGUMENTS);
        }
    }


    /**
     * Read the input source file. If the trailer count matches the number of records, then output output then write the validated file and archive
     * the source file.
     */
    private void run(String jdbcDriver, String dbURL, String dbUserName, String dbPassword ) {      
        LOGGER.logp(Level.INFO, CLASSNAME, "main", "Validating data of CSV file: ");                                     
        File sourceFile = new File(sourceFilePath, validatedFileName);
        long totalLinesCount = FileUtilities.getTotalLinesCount(sourceFile);
        if (totalLinesCount == 0) {
            LOGGER.logp(Level.SEVERE, CLASSNAME, "main", sourceFileName + " is empty");            
            System.exit(EMPTY_FILE);
        }
        String header = readHeader(sourceFile);
        boolean validHeader = validateHeader(header);        
        if (!validHeader) {
            LOGGER.logp(Level.SEVERE, CLASSNAME, "main", "The header " + header + " is not valid, expecting: " + expectedHeader);
            System.exit(INVALID_HEADER);
        }
        try{
            MonitiseEtlHelper  monitiseEtlHelper =  new MonitiseEtlHelper(jdbcDriver, dbURL, dbUserName, dbPassword);
            validateCSVData(monitiseEtlHelper, sourceFile, totalLinesCount);
            monitiseEtlHelper.commit();
            monitiseEtlHelper.close();            
        }catch(SQLException sql){
            LOGGER.logp(Level.SEVERE, CLASSNAME, "main", "SQLException Exception"+sql.getMessage());
            logStackTrace(sql, "run");
            System.exit(NO_SOURCE_FILE_FOUND);        
        }catch(ClassNotFoundException cnfe){
            LOGGER.logp(Level.SEVERE, CLASSNAME, "main", "ClassNotFoundException Exception"+cnfe.getMessage());
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
    public static String readHeader(final File sourceFile) {
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
     * Write the validated output file data (which is the same as the input file without the trailer and with append the source file name).      
     * @param sourceFile - the source file
     * @param lineCount - the number of lines in the source file
     */
    public void validateCSVData(MonitiseEtlHelper  monitiseEtlHelper,File sourceFile, final long lineCount) throws SQLException {
        String methodName = "validateCSVData";
        LOGGER.entering(CLASSNAME, methodName, validatedFileName);
        try {
            
            InputStream fis = getFileInputStream(sourceFile);
            InputStream cleanStream = new UnicodeBOMInputStream(fis).skipBOM();
            InputStreamReader in = new InputStreamReader(cleanStream, "UTF-8");
            BufferedReader br = new BufferedReader(in);
            String cpSku=null;
            int headerLength=0;
            for (int i = 0; i < lineCount; i++) {
                String strLine = br.readLine(); 
                if(i==0){
                    headerLength=strLine.split(DELIMITER).length;
                }
                if(i>0)
                {
                    String[] dataArray=strLine.split(DELIMITER);
                    String eCode="";
                    String seqNo = null;
                    int index=0;                    
                    int rowDataLength=dataArray.length;                    
                    if(headerLength!=rowDataLength){
                        String errorrowDataLength="E016";
                        eCode=(eCode!=null && eCode.length() > 0)? eCode+((errorrowDataLength!="")?COLUMN_DELIMITER+errorrowDataLength:errorrowDataLength):errorrowDataLength;
                     } 
                    
                    String storeentId = "";
                    
                    for(String data : dataArray){
                        switch(index){
                        case CPCODE: 
                            String errorCPCode=monitiseEtlHelper.isValidCpCode(data)!=null?monitiseEtlHelper.isValidCpCode(data):"";
                            eCode=(eCode!=null && eCode.length() > 0)? eCode+((errorCPCode!="")?COLUMN_DELIMITER+errorCPCode:errorCPCode):errorCPCode;
                            if (errorCPCode == null || errorCPCode.trim().length() == 0) {
                                storeentId = monitiseEtlHelper.findStoreentId(data);
                            }
                            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "comments code : "+eCode + " for cpcode");
                            break;
                        case PRODUCTTYPE:
                            String errorProductType=monitiseEtlHelper.isValidPreDefinedValue(data, "productType");
                            errorProductType=(errorProductType==null)? "":errorProductType;
                            eCode=(eCode!=null && eCode.length() > 0)? eCode+((errorProductType!="")?COLUMN_DELIMITER+errorProductType:errorProductType):errorProductType; 
                            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "comments code : "+errorProductType + " for productType");
                            break;    
                         case CPSKU:                             
                             String errorSKU=monitiseEtlHelper.isValidSkuSize(data, "sku")!=null?monitiseEtlHelper.isValidSkuSize(data, "sku"):"";
                             eCode=(eCode!=null && eCode.length() > 0)? eCode+((errorSKU!="")?COLUMN_DELIMITER+errorSKU:errorSKU):errorSKU; 
                             LOGGER.logp(Level.INFO, CLASSNAME, methodName, "comments code : "+errorSKU + " for cpsku");
                             cpSku=data;
                             break;
                        case CPPARENTSKU:
                            if (data != null && data.trim().length() > 0) {
                                String errorParentSKU=monitiseEtlHelper.isValidSkuSize(data, "parentSku")!=null?monitiseEtlHelper.isValidSkuSize(data, "parentSku"):"";
                                eCode=(eCode!=null && eCode.length() > 0)? eCode+((errorParentSKU!="")?COLUMN_DELIMITER+errorParentSKU:errorParentSKU):errorParentSKU; 
                                LOGGER.logp(Level.INFO, CLASSNAME, methodName, "comments code : "+errorParentSKU + " for cpsku");
                            }
                            break;
                        case LANGUAGE:                       
                            String errorLanguage=monitiseEtlHelper.isValidLanguage(data)!=null?monitiseEtlHelper.isValidLanguage(data):"";
                            eCode=(eCode!=null && eCode.length() > 0)? eCode+((errorLanguage!="")?COLUMN_DELIMITER+errorLanguage:errorLanguage):errorLanguage;
                            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "comments code : "+errorLanguage + " for Language");                            
                            break;  
                        case NAME:                            
                            String errorName=monitiseEtlHelper.isValidSkuName(data)!=null?monitiseEtlHelper.isValidSkuName(data):"";
                            eCode=(eCode!=null && eCode.length() > 0)? eCode+((errorName!="")?COLUMN_DELIMITER+errorName:errorName):errorName;
                            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "comments code : "+errorName + " for name");                            
                            break;  
                        case SHORTDESC:
                            String errorShortDesc="";
                            if (data != null && data.trim().length() > 254){
                                errorShortDesc="E021";
                                eCode=(eCode!=null && eCode.length() > 0)? eCode+((errorShortDesc!="")?COLUMN_DELIMITER+errorShortDesc:errorShortDesc):errorShortDesc;
                            }
                            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "comments code : " + errorShortDesc + " for name");                            
                            break;   
                        case LISTPRICE: 
                            String errorListPrice=monitiseEtlHelper.isValidNumericValue(data, LISTPRICECOLNAME)!=null?monitiseEtlHelper.isValidNumericValue(data, LISTPRICECOLNAME):"";
                            errorListPrice=(errorListPrice==null)? "":errorListPrice;
                            eCode=(eCode!=null && eCode.length() > 0)? eCode+((errorListPrice!="")?COLUMN_DELIMITER+errorListPrice:errorListPrice):errorListPrice;
                            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "comments code : "+errorListPrice + " for list price");
                            break;
                        case OFFERPRICE: 
                            String errorOfferPrice=monitiseEtlHelper.isValidNumericValue(data, OFFERPRICECOLNAME)!=null?monitiseEtlHelper.isValidNumericValue(data, OFFERPRICECOLNAME):"";
                            errorOfferPrice=(errorOfferPrice==null)? "":errorOfferPrice;
                            eCode=(eCode!=null && eCode.length() > 0)? eCode+((errorOfferPrice!="")?COLUMN_DELIMITER+errorOfferPrice:errorOfferPrice):errorOfferPrice;
                            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "comments code : "+errorOfferPrice + " for offer price");
                            break;
                        case CURRENCY: 
                            String errorCurrency=monitiseEtlHelper.isValidCurrencyCode(data, Integer.parseInt(storeentId))!=null?monitiseEtlHelper.isValidCurrencyCode(data, Integer.parseInt(storeentId)):"";
                            errorCurrency=(errorCurrency==null)?"":errorCurrency;
                            eCode=(eCode!=null && eCode.length() > 0)? eCode+((errorCurrency!="")?COLUMN_DELIMITER+errorCurrency:errorCurrency):errorCurrency;
                            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "comments code : "+errorCurrency + " for currency");
                            break;
                        case ONSPECIAL:
                            String errorOnSpecial=monitiseEtlHelper.isValidPreDefinedValue(data, "onSpecial");
                            errorOnSpecial=(errorOnSpecial==null)? "":errorOnSpecial;
                            eCode=(eCode!=null && eCode.length() > 0)? eCode+((errorOnSpecial!="")?COLUMN_DELIMITER+errorOnSpecial:errorOnSpecial):errorOnSpecial; 
                            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "comments code : "+errorOnSpecial + " for onSpecial");
                            break;    
                        case PHYSICALSTOCK:
                            String errorPhysicalStock=monitiseEtlHelper.isValidPreDefinedValue(data, "physicalStock");
                            errorPhysicalStock=(errorPhysicalStock==null)? "":errorPhysicalStock;
                            eCode=(eCode!=null && eCode.length() > 0)? eCode+((errorPhysicalStock!="")?COLUMN_DELIMITER+errorPhysicalStock:errorPhysicalStock):errorPhysicalStock; 
                            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "comments code : "+errorPhysicalStock + " for physicalStock");
                            break;
                        case INVENTORY:
                            if(data!=null && !data.equals("")){
                            String errorInventory=monitiseEtlHelper.isValidNumericValue(data, "Inventory");
                            errorInventory=(errorInventory==null)? "":errorInventory;
                            eCode=(eCode!=null && eCode.length() > 0)? eCode+((errorInventory!="")?COLUMN_DELIMITER+errorInventory:errorInventory):errorInventory; 
                            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "comments code : "+errorInventory + " for inventory");
                            }
                            break;    
                        case IMAGE1:
                            String errorImage1="";
                            if (data != null && data.trim().length() > 254){
                                errorImage1="E023:image1";
                                eCode=(eCode!=null && eCode.length() > 0)? eCode+((errorImage1!="")?COLUMN_DELIMITER+errorImage1:errorImage1):errorImage1;
                            }
                            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "comments code : " + errorImage1 + " for image1");                            
                            break;     
                        case IMAGE2:
                            String errorImage2="";
                            if (data != null && data.trim().length() > 254){
                                errorImage2="E023:image2";
                                eCode=(eCode!=null && eCode.length() > 0)? eCode+((errorImage2!="")?COLUMN_DELIMITER+errorImage2:errorImage2):errorImage2;
                            }
                            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "comments code : " + errorImage2 + " for image2");                            
                            break;      
                        case IMAGE3:
                            String errorImage3="";
                            if (data != null && data.trim().length() > 254){
                                errorImage2="E023:image3";
                                eCode=(eCode!=null && eCode.length() > 0)? eCode+((errorImage3!="")?COLUMN_DELIMITER+errorImage3:errorImage3):errorImage3;
                            }
                            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "comments code : " + errorImage3 + " for image3");                            
                            break;       
                        case IMAGE4:
                            String errorImage4="";
                            if (data != null && data.trim().length() > 254){
                                errorImage2="E023:image4";
                                eCode=(eCode!=null && eCode.length() > 0)? eCode+((errorImage4!="")?COLUMN_DELIMITER+errorImage4:errorImage4):errorImage4;
                            }
                            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "comments code : " + errorImage4 + " for image3");                            
                            break; 
                        case STARTDATE:
                            String errorSDate=monitiseEtlHelper.isValidDate(data)!=null?monitiseEtlHelper.isValidDate(data):"";
                            eCode=(eCode!=null && eCode.length() > 0)? eCode+((errorSDate!="")?COLUMN_DELIMITER+errorSDate:errorSDate):errorSDate; 
                            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "comments code : "+errorSDate + " is offer from Date");
                            break;   
                        case ENDDATE:
                            String errorEDate=monitiseEtlHelper.isValidFromAndToDates(data, dataArray[index-1])!=null?monitiseEtlHelper.isValidFromAndToDates(data, dataArray[index-1]):"";
                            eCode=(eCode!=null && eCode.length() > 0)? eCode+((errorEDate!="")?COLUMN_DELIMITER+errorEDate:errorEDate):errorEDate; 
                            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "comments code : "+errorEDate + " is offer from and toDate");
                            break;    
                        case AGERESTRICTION:
                            if(data!=null && !data.equals("")){
                                String errorAgeRestriction=monitiseEtlHelper.isValidInteger(data, "ageRestriction");
                                errorAgeRestriction=(errorAgeRestriction==null)? "":errorAgeRestriction;
                                eCode=(eCode!=null && eCode.length() > 0)? eCode+((errorAgeRestriction!="")?COLUMN_DELIMITER+errorAgeRestriction:errorAgeRestriction):errorAgeRestriction; 
                                LOGGER.logp(Level.INFO, CLASSNAME, methodName, "comments code : "+errorAgeRestriction + " for ageRestriction");
                            }
                            break;    
                        case KEYWORD:
                            String errorKeyword="";
                            if (data != null && data.trim().length() > 254){
                                errorKeyword="E023:keyword";
                                eCode=(eCode!=null && eCode.length() > 0)? eCode+((errorKeyword!="")?COLUMN_DELIMITER+errorKeyword:errorKeyword):errorKeyword;
                            }
                            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "comments code : " + errorKeyword + " for name");                            
                            break;     
                        case SEQUENCE_NO:
                            seqNo = dataArray[index];
                            break;
                        }
                      index++;
                    }
                    String status=(eCode!=null && eCode.length() > 0)?"2":"1";
                    monitiseEtlHelper.updateErrorCodeBySeq(TableName.XINT_OFFERSDATA, cpSku, seqNo, eCode, status);                             
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