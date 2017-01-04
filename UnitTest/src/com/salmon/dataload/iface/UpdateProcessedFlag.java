package com.salmon.dataload.iface;

import static com.salmon.dataload.iface.DataLoadConstants.INVALID_ARGUMENTS;
import static com.salmon.dataload.iface.DataLoadConstants.NO_SOURCE_FILE_FOUND;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
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

public class UpdateProcessedFlag {
    
    private static final int NUM_ARGUMENTS  = 7;
    private static final int TABLE_NAME     = 0;
    private static final int JDBC_DRIVER    = 1;
    private static final int DB_URL         = 2;
    private static final int DB_USERNAME    = 3;
    private static final int DB_PASSWORD    = 4;
    private static final int SOURCEFILEPATH = 5;
    private static final int SOURCEFILENAME = 6;
    
    static final String DELIMITER = ",";
    
    private static final String CLASSNAME = UpdateProcessedFlag.class.getName();
    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(CLASSNAME);
    
    
    public static void main(final String[] args) {
        
        String methodName = "main";
        
        if (args.length == NUM_ARGUMENTS && args[TABLE_NAME].length() > 0 && args[JDBC_DRIVER].length() > 0 
                && args[DB_URL].length() > 0 && args[DB_USERNAME].length() > 0 && args[DB_PASSWORD].length() > 0 
                && args[SOURCEFILEPATH].length() > 0 && args[SOURCEFILENAME].length() > 0)
        {
        LOGGER.logp(Level.INFO, CLASSNAME, methodName, "source file: " + args[SOURCEFILENAME]);
        UpdateProcessedFlag updateflag = new UpdateProcessedFlag();
        updateflag.run(args[TABLE_NAME], args[JDBC_DRIVER], args[DB_URL], args[DB_USERNAME], args[DB_PASSWORD], args[SOURCEFILEPATH], args[SOURCEFILENAME] );
        
    }else{
        LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "Invalid arguments passed to UpdateProcessedFlag");
        System.exit(INVALID_ARGUMENTS);
    }
}
    
    private void run(String tableName, String jdbcDriver, String dbURL, String dbUserName, String dbPassword, String sourceFilePath, String sourceFileName) {
        
        String methodName = "run";
        
        LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Updating processed flag: ");   
        String[] dataArray=null;
        String strLine =null;
        try{
        File sourceFile = new File(sourceFilePath, sourceFileName);  
        FileInputStream fis = new FileInputStream(sourceFile);
        long totalLinesCount = FileUtilities.getTotalLinesCount(sourceFile);
        InputStream cleanStream = new UnicodeBOMInputStream(fis).skipBOM();
        InputStreamReader in = new InputStreamReader(cleanStream, "UTF-8");
        BufferedReader br = new BufferedReader(in);
        MonitiseEtlHelper  monitiseEtlHelper =  new MonitiseEtlHelper(jdbcDriver, dbURL, dbUserName, dbPassword);
        for (int i = 0; i < totalLinesCount; i++) {
            strLine = br.readLine();
            if(i>0)
            {   
                strLine=strLine.replaceAll("\\\"", "");
                dataArray=strLine.split(DELIMITER);
                LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Record read"+strLine );                   
        
                if(tableName.equals(TableName.XINT_STOCKDATA.name())){
                    LOGGER.logp(Level.INFO, CLASSNAME, methodName, "XINT_STOCKDATA");
                    monitiseEtlHelper.updateProcessedFlag(TableName.XINT_STOCKDATA, dataArray);
                }else if(tableName.equals(TableName.XINT_PRICEDATA.name())){
                    LOGGER.logp(Level.INFO, CLASSNAME, methodName, "table name XINT_PRICEDATA");
                    monitiseEtlHelper.updateProcessedFlag(TableName.XINT_PRICEDATA, dataArray);
                }else if(tableName.equals(TableName.XINT_STORESDATA.name())){
                    LOGGER.logp(Level.INFO, CLASSNAME, methodName, "table name XINT_STORESDATA");
                    monitiseEtlHelper.updateProcessedFlag(TableName.XINT_STORESDATA, dataArray);
                }else if(tableName.equals(TableName.XINT_MASDATA.name())){
                    LOGGER.logp(Level.INFO, CLASSNAME, methodName, "table name XINT_MASDATA");
                    monitiseEtlHelper.updateProcessedFlag(TableName.XINT_MASDATA, dataArray);
                }else if(tableName.equals(TableName.XINT_OFFERSDATA.name())){
                    LOGGER.logp(Level.INFO, CLASSNAME, methodName, "table name XINT_OFFERSDATA");
                    monitiseEtlHelper.updateProcessedFlag(TableName.XINT_OFFERSDATA, dataArray);
                }else if(tableName.equals(TableName.XINT_LANGDATA.name())){
                    LOGGER.logp(Level.INFO, CLASSNAME, methodName, "table name XINT_LANGDATA");
                    monitiseEtlHelper.updateProcessedFlag(TableName.XINT_LANGDATA, dataArray);
                }else if(tableName.equals(TableName.XINT_ATTRIBUTEDATA.name())){
                    LOGGER.logp(Level.INFO, CLASSNAME, methodName, "table name XINT_ATTRIBUTEDATA");
                    monitiseEtlHelper.updateProcessedFlag(TableName.XINT_ATTRIBUTEDATA, dataArray);
                }else if(tableName.equals(TableName.XINT_PROMOTIONDATA.name())){
                    LOGGER.logp(Level.INFO, CLASSNAME, methodName, "table name XINT_PROMOTIONDATA");
                    monitiseEtlHelper.updateProcessedFlag(TableName.XINT_PROMOTIONDATA, dataArray);
                }
            }
        }
        monitiseEtlHelper.commit();
        monitiseEtlHelper.close(); 
        br.close();
        in.close();
        fis.close();
            
        }catch(SQLException sql){
            LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "SQLException Exception"+sql.getMessage());
            logStackTrace(sql, "run");
            System.exit(NO_SOURCE_FILE_FOUND);        
        }catch(ClassNotFoundException cnfe){
            LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "ClassNotFoundException Exception"+cnfe.getMessage());
            logStackTrace(cnfe, methodName);
            System.exit(NO_SOURCE_FILE_FOUND);        
        }
        catch(Exception e){
            LOGGER.logp(Level.SEVERE, CLASSNAME, "main", "Exception"+e.getMessage());
            logStackTrace(e, methodName);
            System.exit(NO_SOURCE_FILE_FOUND);        
        }
        LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Exiting");
      
    }
    
    private static void logStackTrace(final Throwable aThrowable, final String methodName) {
        Writer result = new StringWriter();
        aThrowable.printStackTrace(new PrintWriter(result));
        LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, result.toString());
    }

}
