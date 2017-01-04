package com.salmon.dataload.iface;

import static com.salmon.dataload.iface.DataLoadConstants.ERROR_WRITING_ARCHIVE_FILE;
import static com.salmon.dataload.iface.DataLoadConstants.ERROR_WRITING_TARGET_FILE;
import static com.salmon.dataload.iface.DataLoadConstants.INVALID_ARGUMENTS;
import static com.salmon.dataload.iface.DataLoadConstants.NO_SOURCE_FILE_FOUND;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.logging.Level;

import com.salmon.dataload.helper.PromotionEtlHelper;
import com.salmon.dataload.helper.TableName;
import com.salmon.dataload.utils.FileUtilities;

/**
 * This class fetches the data from PX_PROMOTION and create insert sql so that promotion becomes visible from Management center.
 * 
 * @author Keshav Chawla
 * @revision : 1.0
 * @Date : 27 July 2014
 */
public class InsertPromotionSQLFile {
    
    private static final int NUM_ARGUMENTS = 5;
    private static final int SOURCE_DIR = 0;
    private static final int JDBC_DRIVER = 1;
    private static final int DB_URL = 2;
    private static final int DB_USERNAME = 3;
    private static final int DB_PASSWORD = 4;    
    private static final String CLASSNAME = InsertPromotionSQLFile.class.getName();
    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(CLASSNAME);
    private static final String ARCHIVE_FOLDER = "archive";
    private static final String FILE_NAME="promotion.xml";

    private String jdbcDriver;
    private String dbUrl;
    private String dbUserName;
    private String dbPassword;
    
    PromotionEtlHelper promotionEtlHelper = null;
    
    
    
    public PromotionEtlHelper getPromotionEtlHelper() throws ClassNotFoundException, SQLException {
        if (promotionEtlHelper == null) {
            promotionEtlHelper = new PromotionEtlHelper(jdbcDriver, dbUrl, dbUserName, dbPassword);
        }
        return promotionEtlHelper;
    }

    public void setPromotionEtlHelper(PromotionEtlHelper promotionEtlHelper) {
        this.promotionEtlHelper = promotionEtlHelper;
    }

    /**
     * 
     * @param sourceDirectory
     * @param jdbcDriver
     * @param dbURL
     * @param dbUserName
     * @param dbPassword
     * @throws SQLException
     */
    public static void main(final String[] args) throws SQLException {
        
        if (args.length == NUM_ARGUMENTS && args[JDBC_DRIVER].length() > 0 
                && args[DB_URL].length() > 0 && args[DB_USERNAME].length() > 0 && args[DB_PASSWORD].length() > 0 )
                
        {
            LOGGER.logp(Level.INFO, CLASSNAME, "main", "Process to create a promotion xml ");
            
            int i = 0;
            for(String arg : args) {
                LOGGER.logp(Level.INFO, CLASSNAME, "main", "Arg["+i++ +"] = " + arg);
            }
            
            InsertPromotionSQLFile createPromotion = new InsertPromotionSQLFile();
            createPromotion.run(args[SOURCE_DIR], args[JDBC_DRIVER], args[DB_URL], args[DB_USERNAME], args[DB_PASSWORD]);
           // createPromotion.run("C:\\WCDE_ENT70\\samples\\promotion\\ImportExport.windows\\promotions","com.ibm.db2.jcc.DB2Driver", "jdbc:db2://localhost:50000/Storedb", "wcsadmin", "WcsAdm1n");
            
        }else{
            LOGGER.logp(Level.SEVERE, CLASSNAME, "main", "Invalid arguments passed to CreatePromotionXMLFile");
            System.exit(INVALID_ARGUMENTS);
        }
/*        LOGGER.logp(Level.INFO, CLASSNAME, "main", "Process to create a promotion xml ");
        InsertPromotionSQLFile createPromotion = new InsertPromotionSQLFile();
        createPromotion.run("C:\\WCDE_ENT70\\samples\\promotion\\ImportExport.windows\\promotions","com.ibm.db2.jcc.DB2Driver", "jdbc:db2://localhost:50000/Storedb", "wcsadmin", "WcsAdm1n");       */             
    }
        
        /**
         * @param sourceDirectory
         * @param jdbcDriver
         * @param dbURL
         * @param dbUserName
         * @param dbPassword
         * @throws SQLException
         */
        public void run(String sourceDirectory,String jdbcDriver, String dbURL, String dbUserName, String dbPassword) throws SQLException {      
            LOGGER.logp(Level.INFO, CLASSNAME, "main", "Creating promotion XML"); 
                  
            this.jdbcDriver = jdbcDriver;
            this.dbUrl = dbURL;
            this.dbUserName = dbUserName;
            this.dbPassword = dbPassword;
            
            try{                              
                String[]  promotionXML = getPromotionEtlHelper().createPromotionXML();    
               
                createPromotionSQL(promotionXML,sourceDirectory);
                getPromotionEtlHelper().commit();
                getPromotionEtlHelper().close();            
                
            }catch(SQLException sql){
                LOGGER.logp(Level.SEVERE, CLASSNAME, "main", "SQLException Exception"+sql.getMessage());
                
                System.exit(NO_SOURCE_FILE_FOUND);        

            } catch (ClassNotFoundException e) {
                LOGGER.logp(Level.SEVERE, CLASSNAME, "main", "ClassNotFoundException Exception"+e.getMessage());            
                System.exit(ERROR_WRITING_TARGET_FILE); 
            }

            LOGGER.logp(Level.INFO, CLASSNAME, "Creating promotion xml", "Exiting");
          
        }

        
        
        
        /**
         * This method created a promotion XML file from XINT_PROMOTIONDATA table
         * and will be used by the OOB utility to load the promotion.
         * 
         * @throws SQLException Exception
         */
        private void insertPromoSql(final String insertSql) throws SQLException, ClassNotFoundException {
            String methodName = "insertPromoSql";
            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "SQL:" + insertSql);
            PreparedStatement pstmt  = null;       
            try {
                pstmt = getPromotionEtlHelper().getJdbcConnection().prepareStatement(insertSql);
                pstmt.executeUpdate();
            } catch (SQLException sqle) {
                throw sqle;
            } finally {
                if (pstmt != null) {
                    pstmt.close();
                    pstmt = null;
                }
            }
        }        
        
        /**
         * @param sourceDirectory
         * @param promoId
         * @throws SQLException
         */
        private void createPromotionSQL(String[] promotionXMLData,String srcDirectory) 
        throws SQLException, ClassNotFoundException {      
            LOGGER.logp(Level.INFO, CLASSNAME, "main", "Creating insert scripts for promotion"); 
            
            String promoId = promotionXMLData[1];                 
            String cpCode = promotionXMLData[2]; 
            String cpSKU = promotionXMLData[3];
            String productType = promotionXMLData[4];
            String percentageOff = promotionXMLData[5];
            String amountOff = promotionXMLData[6]; 
            String shipQualAmount = promotionXMLData[7];
            String promotionName = promotionXMLData[8];
            String insertPxElementSql = null;
            String insertPxElementSql1 = null;
            String insertPxElementSql2 = null;
            String insertPxElementSql3 = null;
            String insertPxElementSql4 = null;
            String insertPxElementNVPSql = null;
            String insertPxElementNVPSql1 = null;
            String insertPxElementNVPSql2 = null;
            String insertPxElementNVPSql3 = null;
            String insertPxElementNVPSql4 = null;
            String insertPxElementNVPSql5 = null;
            String insertPxElementNVPSql6 = null;
            String insertPxElementNVPSql7 = null;
            String insertPxElementNVPSql8 = null;
            String insertPxElementNVPSql9 = null;
            String insertPxElementNVPSql10 = null;

            
            String insertPxPromoAuthSql = null;
            //productType="P";
            
            try{
                String  catentryId = getPromotionEtlHelper().getCatentryId(cpSKU);  
                String maxPxElement= getPromotionEtlHelper().maxPxElementId(promoId);                
                int intMaxPxElement = Integer.parseInt(maxPxElement);
                String maxPxElementNVP = getPromotionEtlHelper().maxPxElementNVPId(promoId);
                int intMaxPxElementNVP = Integer.parseInt(maxPxElementNVP);
                String promoName = getPromotionEtlHelper().getPxPromotionId(promotionName);
                //catentryId = "10004";
                              
                if (productType.equals("P") && amountOff!=null){
                insertPxElementSql = "INSERT INTO PX_ELEMENT (PX_ELEMENT_ID, PX_PROMOTION_ID, NAME, TYPE, SUBTYPE, PARENT, SEQUENCE, OPTCOUNTER) VALUES ( " + (intMaxPxElement+1)  + ", "+promoName+", "+ (intMaxPxElement+1)+", 'PurchaseCondition', 'ProductLevelPerItemValueDiscountPurchaseCondition', null, 0.0, 0)";
                insertPxElementSql1 = "INSERT INTO PX_ELEMENT (PX_ELEMENT_ID, PX_PROMOTION_ID, NAME, TYPE, SUBTYPE, PARENT, SEQUENCE, OPTCOUNTER) VALUES (" + (intMaxPxElement+2) + ", "+promoName+", " + (intMaxPxElement+2) + ", 'DiscountRange', 'ProductLevelFixedAmountOffDiscountRange', " + (intMaxPxElement+1) + ", 1.0, 0)"; 
                insertPxElementSql2 = "INSERT INTO PX_ELEMENT (PX_ELEMENT_ID, PX_PROMOTION_ID, NAME, TYPE, SUBTYPE, PARENT, SEQUENCE, OPTCOUNTER) VALUES (" + (intMaxPxElement+3) + ", "+promoName+", " + (intMaxPxElement+3) + ", 'IncludePaymentTypeIdentifier', 'Identifier_PaymentType', "+ (intMaxPxElement+1)+", 0.0, 0)"; 
                insertPxElementSql3 = "INSERT INTO PX_ELEMENT (PX_ELEMENT_ID, PX_PROMOTION_ID, NAME, TYPE, SUBTYPE, PARENT, SEQUENCE, OPTCOUNTER) VALUES (" + (intMaxPxElement+4) + ", "+promoName+", " + (intMaxPxElement+4) + ", 'IncludeCatalogEntryIdentifier', 'Identifier_InheritedCatalogEntry', " + (intMaxPxElement+1) + ", 0.0, 0)" ;
                insertPxElementSql4 = "INSERT INTO PX_ELEMENT (PX_ELEMENT_ID, PX_PROMOTION_ID, NAME, TYPE, SUBTYPE, PARENT, SEQUENCE, OPTCOUNTER) VALUES (" + (intMaxPxElement+5) + ", "+promoName+", " + (intMaxPxElement+5) + ", 'TargetingCondition', 'TargetingCondition', null, 0.0, 0)";

                
                insertPxElementNVPSql1  =  "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+3)  + ", " + (intMaxPxElement+1)  + ", 'CatalogEntryAttributeRuleCaseSensitive', 'false', 0)"  ;
                insertPxElementNVPSql2 = "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+4)  + ", " + (intMaxPxElement+1)  + ", 'Currency', 'GBP', 0)" ;
                insertPxElementNVPSql3 = "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+1)  + ", " + (intMaxPxElement+1)  + ", 'Language', '44', 0)"  ;
                insertPxElementNVPSql4 = "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+6)  + ", " + (intMaxPxElement+1)  + ", 'MinimumPurchaseType', 'Quantity', 0)" ;
                insertPxElementNVPSql5 = "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+5)  + ", " + (intMaxPxElement+1)  + ", 'PriceAdjustmentBase', '-3', 0)"; 
                insertPxElementNVPSql6 = "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+2)  + ", " + (intMaxPxElement+1)  + ", 'siteWide', 'false', 0)"  ;
                insertPxElementNVPSql7  = "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+8)  + ", " + (intMaxPxElement+2)  + ", 'AmountOff', "+amountOff+", 0)"  ;
                insertPxElementNVPSql8  = "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+7)  + ", " + (intMaxPxElement+2)  + ", 'LowerBound', '1', 0)"  ;
                insertPxElementNVPSql9 = "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+9)  + ", " + (intMaxPxElement+3)  + ", 'PaymentType', 'Any', 0)";
                insertPxElementNVPSql10  = "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+10)  + ", " + (intMaxPxElement+4)  + ", 'Id', "+catentryId+ ", 0)" ;
                
                
                insertPxPromoAuthSql = "INSERT INTO PX_PROMOAUTH (PX_PROMOTION_ID, COMMENTS, PROMOTIONTYPE, DAILYSTARTTIME, DAILYENDTIME, WEEKDAY_SUN, WEEKDAY_MON, WEEKDAY_TUE, WEEKDAY_WED, WEEKDAY_THU, WEEKDAY_FRI, WEEKDAY_SAT, CTLPARAM, OPTCOUNTER, ADMINSTVENAME) VALUES ("+promoName+", 'null', 'ProductLevelPerItemValueDiscount', '2000-01-01 00:00:00', '2000-01-01 23:59:59', 1, 1, 1, 1, 1, 1, 1, 'CMC', 0,'" + promotionName + "')";
                
                
                insertPromoSql(insertPxElementSql);
                insertPromoSql(insertPxElementSql1);
                insertPromoSql(insertPxElementSql2);
                insertPromoSql(insertPxElementSql3);
                insertPromoSql(insertPxElementSql4);
                                
                insertPromoSql(insertPxElementNVPSql1);
                insertPromoSql(insertPxElementNVPSql2);
                insertPromoSql(insertPxElementNVPSql3);
                insertPromoSql(insertPxElementNVPSql4);
                insertPromoSql(insertPxElementNVPSql5);                
                insertPromoSql(insertPxElementNVPSql6);
                insertPromoSql(insertPxElementNVPSql7);
                insertPromoSql(insertPxElementNVPSql8);
                insertPromoSql(insertPxElementNVPSql9);
                insertPromoSql(insertPxElementNVPSql10);
                              
                insertPromoSql(insertPxPromoAuthSql);
                updateStatus(promoId);
                getPromotionEtlHelper().commit();
                getPromotionEtlHelper().close();
                archive(FILE_NAME,srcDirectory);

                }
                else if (productType.equals("P") && percentageOff!=null) {
                    insertPxElementSql = "INSERT INTO PX_ELEMENT (PX_ELEMENT_ID, PX_PROMOTION_ID, NAME, TYPE, SUBTYPE, PARENT, SEQUENCE, OPTCOUNTER) VALUES ( " + (intMaxPxElement+2)  + ", "+promoName+", "+ (intMaxPxElement+2)+", 'PurchaseCondition', 'ProductLevelPerItemPercentDiscountPurchaseCondition', null, 0.0, 0)";
                    insertPxElementSql1 = "INSERT INTO PX_ELEMENT (PX_ELEMENT_ID, PX_PROMOTION_ID, NAME, TYPE, SUBTYPE, PARENT, SEQUENCE, OPTCOUNTER) VALUES (" + (intMaxPxElement+3) + ", "+promoName+", " + (intMaxPxElement+3) + ", 'DiscountRange', 'ProductLevelPercentOffDiscountRange', " + (intMaxPxElement+2)  + ", 1.0, 0)";
                    insertPxElementSql2 = "INSERT INTO PX_ELEMENT (PX_ELEMENT_ID, PX_PROMOTION_ID, NAME, TYPE, SUBTYPE, PARENT, SEQUENCE, OPTCOUNTER) VALUES (" + (intMaxPxElement+5) + ", "+promoName+", " + (intMaxPxElement+5) + ", 'IncludePaymentTypeIdentifier', 'Identifier_PaymentType', " + (intMaxPxElement+2)  + ", 0.0, 0)" ;
                    insertPxElementSql3 = "INSERT INTO PX_ELEMENT (PX_ELEMENT_ID, PX_PROMOTION_ID, NAME, TYPE, SUBTYPE, PARENT, SEQUENCE, OPTCOUNTER) VALUES (" + (intMaxPxElement+4) + ", "+promoName+", " + (intMaxPxElement+4) + ", 'IncludeCatalogEntryIdentifier', 'Identifier_InheritedCatalogEntry', " + (intMaxPxElement+2)  + ", 0.0, 0)"; 
                    insertPxElementSql4 = "INSERT INTO PX_ELEMENT (PX_ELEMENT_ID, PX_PROMOTION_ID, NAME, TYPE, SUBTYPE, PARENT, SEQUENCE, OPTCOUNTER) VALUES (" + (intMaxPxElement+1) + ", "+promoName+", " + (intMaxPxElement+1) + ", 'TargetingCondition', 'TargetingCondition', null, 0.0, 0)";
               
                    insertPxElementNVPSql1  = "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+3)  + ", " + (intMaxPxElement+2)  + ", 'CatalogEntryAttributeRuleCaseSensitive', 'false', 0)";
                    insertPxElementNVPSql2 = "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+4)  + ", " + (intMaxPxElement+2)  + ", 'Currency', 'GBP', 0)" ;
                    insertPxElementNVPSql3 = "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+1)  + ", " + (intMaxPxElement+2)  + ", 'Language', '44', 0)" ;
                    insertPxElementNVPSql4 = "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+6)  + ", " + (intMaxPxElement+2)  + ", 'MinimumPurchaseType', 'Quantity', 0)";
                    insertPxElementNVPSql5 = "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+5)  + ", " + (intMaxPxElement+2)  + ", 'PriceAdjustmentBase', '-3', 0)"; 
                    insertPxElementNVPSql6 = "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+2)  + ", " + (intMaxPxElement+2)  + ", 'siteWide', 'false', 0)";
                    insertPxElementNVPSql7 = "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+8)  + ", " + (intMaxPxElement+3)  + ", 'LowerBound', '1', 0)";
                    insertPxElementNVPSql8 = "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+7)  + ", " + (intMaxPxElement+3)  + ", 'Percentage', "+percentageOff+", 0)";
                    insertPxElementNVPSql9 = "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+9)  + ", " + (intMaxPxElement+4)  + ", 'Id', "+catentryId+ ", 0)";
                    insertPxElementNVPSql10 = "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+10)  + ", " + (intMaxPxElement+5)  + ", 'PaymentType', 'Any', 0)" ;
                 
                    insertPxPromoAuthSql = "INSERT INTO PX_PROMOAUTH (PX_PROMOTION_ID, COMMENTS, PROMOTIONTYPE, DAILYSTARTTIME, DAILYENDTIME, WEEKDAY_SUN, WEEKDAY_MON, WEEKDAY_TUE, WEEKDAY_WED, WEEKDAY_THU, WEEKDAY_FRI, WEEKDAY_SAT, CTLPARAM, OPTCOUNTER, ADMINSTVENAME) VALUES ("+promoName+", 'null', 'ProductLevelPerItemPercentDiscount', '2000-01-01 00:00:00', '2000-01-01 23:59:59', 1, 1, 1, 1, 1, 1, 1, 'CMC', 0,'" + promotionName + "')"; 
                    
                    insertPromoSql(insertPxElementSql);
                    insertPromoSql(insertPxElementSql1);
                    insertPromoSql(insertPxElementSql2);
                    insertPromoSql(insertPxElementSql3);
                    insertPromoSql(insertPxElementSql4);
                                    
                    insertPromoSql(insertPxElementNVPSql1);
                    insertPromoSql(insertPxElementNVPSql2);
                    insertPromoSql(insertPxElementNVPSql3);
                    insertPromoSql(insertPxElementNVPSql4);
                    insertPromoSql(insertPxElementNVPSql5);                
                    insertPromoSql(insertPxElementNVPSql6);
                    insertPromoSql(insertPxElementNVPSql7);
                    insertPromoSql(insertPxElementNVPSql8);
                    insertPromoSql(insertPxElementNVPSql9);
                    insertPromoSql(insertPxElementNVPSql10);
                                  
                    insertPromoSql(insertPxPromoAuthSql);
                    updateStatus(promoId);
                    
                    getPromotionEtlHelper().commit();
                    getPromotionEtlHelper().close();
                    archive(FILE_NAME,srcDirectory);
                }
                else if (productType.equals("S") && shipQualAmount!=null && amountOff==null){
                    
                    
                    insertPxElementSql =  "INSERT INTO PX_ELEMENT (PX_ELEMENT_ID, PX_PROMOTION_ID, NAME, TYPE, SUBTYPE, PARENT, SEQUENCE, OPTCOUNTER) VALUES (" + (intMaxPxElement+1)  + ", "+promoName+", "+ (intMaxPxElement+1)+", 'TargetingCondition', 'TargetingCondition', null, 0.0, 0)";
                    insertPxElementSql1= "INSERT INTO PX_ELEMENT (PX_ELEMENT_ID, PX_PROMOTION_ID, NAME, TYPE, SUBTYPE, PARENT, SEQUENCE, OPTCOUNTER) VALUES (" + (intMaxPxElement+2)  + ", "+promoName+", "+ (intMaxPxElement+2)+", 'PurchaseCondition', 'OrderLevelFixedAmountOffShippingDiscountPurchaseCondition', null, 0.0, 0)";
                    insertPxElementSql2="INSERT INTO PX_ELEMENT (PX_ELEMENT_ID, PX_PROMOTION_ID, NAME, TYPE, SUBTYPE, PARENT, SEQUENCE, OPTCOUNTER) VALUES (" + (intMaxPxElement+3)  + ", "+promoName+", "+ (intMaxPxElement+3)+", 'IncludePaymentTypeIdentifier', 'Identifier_PaymentType', "+ (intMaxPxElement+2)+", 0.0, 0)";
                    insertPxElementSql3="INSERT INTO PX_ELEMENT (PX_ELEMENT_ID, PX_PROMOTION_ID, NAME, TYPE, SUBTYPE, PARENT, SEQUENCE, OPTCOUNTER) VALUES (" + (intMaxPxElement+4)  + ", "+promoName+", "+ (intMaxPxElement+4)+", 'AnyShipMode', 'Identifier_ShipMode', "+ (intMaxPxElement+2)+", 0.0, 0)"; 
                    
                    insertPxElementNVPSql1 = "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+1)  + ", " + (intMaxPxElement+2)  + ", 'Language', '44', 0)" ;
                    insertPxElementNVPSql2 = "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+2)  + ", " + (intMaxPxElement+2)  + ", 'MinimumPurchase', '1', 0)";
                    insertPxElementNVPSql3 = "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+3)  + ", " + (intMaxPxElement+2)  + ", 'AmountOff', "+shipQualAmount+", 0)";
                    insertPxElementNVPSql4 = "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+4)  + ", " + (intMaxPxElement+2)  + ", 'AdjustmentType', 'IndividualAffectedItems', 0)" ;
                    insertPxElementNVPSql5 = "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+5)  + ", " + (intMaxPxElement+2)  + ", 'Currency', 'GBP', 0)"  ;
                    insertPxElementNVPSql6 = "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+6)  + ", " + (intMaxPxElement+3)  + ", 'PaymentType', 'Any', 0)";
                    
                    insertPxPromoAuthSql = "INSERT INTO PX_PROMOAUTH (PX_PROMOTION_ID, COMMENTS, PROMOTIONTYPE, DAILYSTARTTIME, DAILYENDTIME, WEEKDAY_SUN, WEEKDAY_MON, WEEKDAY_TUE, WEEKDAY_WED, WEEKDAY_THU, WEEKDAY_FRI, WEEKDAY_SAT, CTLPARAM, OPTCOUNTER, ADMINSTVENAME) VALUES ("+promoName+", 'null', 'OrderLevelFixedAmountOffShippingDiscount', '2000-01-01 00:00:00', '2000-01-01 23:59:59', 1, 1, 1, 1, 1, 1, 1, 'CMC', 0,'" + promotionName + "')";
                    
                    insertPromoSql(insertPxElementSql);
                    insertPromoSql(insertPxElementSql1);
                    insertPromoSql(insertPxElementSql2);
                    insertPromoSql(insertPxElementSql3);
                                    
                    insertPromoSql(insertPxElementNVPSql1);
                    insertPromoSql(insertPxElementNVPSql2);
                    insertPromoSql(insertPxElementNVPSql3);
                    insertPromoSql(insertPxElementNVPSql4);
                    insertPromoSql(insertPxElementNVPSql5);                
                    insertPromoSql(insertPxElementNVPSql6);
                                  
                    insertPromoSql(insertPxPromoAuthSql);
                    updateStatus(promoId);
                    
                    getPromotionEtlHelper().commit();
                    getPromotionEtlHelper().close();
                    archive(FILE_NAME,srcDirectory);
                    
                }
                
                else if (productType.equals("S") && shipQualAmount!=null && amountOff!=null){
                    
                    
                    insertPxElementSql =  "INSERT INTO PX_ELEMENT (PX_ELEMENT_ID, PX_PROMOTION_ID, NAME, TYPE, SUBTYPE, PARENT, SEQUENCE, OPTCOUNTER) VALUES (" + (intMaxPxElement+1)  + ", "+promoName+", "+ (intMaxPxElement+1)+", 'TargetingCondition', 'TargetingCondition', null, 0.0, 0)";
                    insertPxElementSql1= "INSERT INTO PX_ELEMENT (PX_ELEMENT_ID, PX_PROMOTION_ID, NAME, TYPE, SUBTYPE, PARENT, SEQUENCE, OPTCOUNTER) VALUES (" + (intMaxPxElement+2)  + ", "+promoName+", "+ (intMaxPxElement+2)+", 'PurchaseCondition', 'OrderLevelFixedAmountOffShippingDiscountPurchaseCondition', null, 0.0, 0)";
                    insertPxElementSql2="INSERT INTO PX_ELEMENT (PX_ELEMENT_ID, PX_PROMOTION_ID, NAME, TYPE, SUBTYPE, PARENT, SEQUENCE, OPTCOUNTER) VALUES (" + (intMaxPxElement+3)  + ", "+promoName+", "+ (intMaxPxElement+3)+", 'IncludePaymentTypeIdentifier', 'Identifier_PaymentType', "+ (intMaxPxElement+2)+", 0.0, 0)";
                    insertPxElementSql3="INSERT INTO PX_ELEMENT (PX_ELEMENT_ID, PX_PROMOTION_ID, NAME, TYPE, SUBTYPE, PARENT, SEQUENCE, OPTCOUNTER) VALUES (" + (intMaxPxElement+4)  + ", "+promoName+", "+ (intMaxPxElement+4)+", 'AnyShipMode', 'Identifier_ShipMode', "+ (intMaxPxElement+2)+", 0.0, 0)"; 
                    
                    insertPxElementNVPSql1 = "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+1)  + ", " + (intMaxPxElement+2)  + ", 'Language', '44', 0)" ;
                    insertPxElementNVPSql2 = "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+2)  + ", " + (intMaxPxElement+2)  + ", 'MinimumPurchase'," + shipQualAmount + ", 0)";
                    insertPxElementNVPSql3 = "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+3)  + ", " + (intMaxPxElement+2)  + ", 'AmountOff', "+amountOff+", 0)";
                    insertPxElementNVPSql4 = "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+4)  + ", " + (intMaxPxElement+2)  + ", 'AdjustmentType', 'IndividualAffectedItems', 0)" ;
                    insertPxElementNVPSql5 = "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+5)  + ", " + (intMaxPxElement+2)  + ", 'Currency', 'GBP', 0)"  ;
                    insertPxElementNVPSql6 = "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+6)  + ", " + (intMaxPxElement+3)  + ", 'PaymentType', 'Any', 0)";
                    
                    insertPxPromoAuthSql = "INSERT INTO PX_PROMOAUTH (PX_PROMOTION_ID, COMMENTS, PROMOTIONTYPE, DAILYSTARTTIME, DAILYENDTIME, WEEKDAY_SUN, WEEKDAY_MON, WEEKDAY_TUE, WEEKDAY_WED, WEEKDAY_THU, WEEKDAY_FRI, WEEKDAY_SAT, CTLPARAM, OPTCOUNTER, ADMINSTVENAME) VALUES ("+promoName+", 'null', 'OrderLevelFixedAmountOffShippingDiscount', '2000-01-01 00:00:00', '2000-01-01 23:59:59', 1, 1, 1, 1, 1, 1, 1, 'CMC', 0,'" + promotionName + "')";
                    
                    insertPromoSql(insertPxElementSql);
                    insertPromoSql(insertPxElementSql1);
                    insertPromoSql(insertPxElementSql2);
                    insertPromoSql(insertPxElementSql3);
                                    
                    insertPromoSql(insertPxElementNVPSql1);
                    insertPromoSql(insertPxElementNVPSql2);
                    insertPromoSql(insertPxElementNVPSql3);
                    insertPromoSql(insertPxElementNVPSql4);
                    insertPromoSql(insertPxElementNVPSql5);                
                    insertPromoSql(insertPxElementNVPSql6);
                                  
                    insertPromoSql(insertPxPromoAuthSql);
                    updateStatus(promoId);
                    
                    getPromotionEtlHelper().commit();
                    getPromotionEtlHelper().close();
                    archive(FILE_NAME,srcDirectory);
                    
                }
                else if (productType.equals("S")  && amountOff!=null && shipQualAmount==null ){
                    insertPxElementSql =  "INSERT INTO PX_ELEMENT (PX_ELEMENT_ID, PX_PROMOTION_ID, NAME, TYPE, SUBTYPE, PARENT, SEQUENCE, OPTCOUNTER) VALUES (" + (intMaxPxElement+1)  + ", "+promoName+", "+ (intMaxPxElement+1)+", 'TargetingCondition', 'TargetingCondition', null, 0.0, 0)" ;
                    insertPxElementSql1 = "INSERT INTO PX_ELEMENT (PX_ELEMENT_ID, PX_PROMOTION_ID, NAME, TYPE, SUBTYPE, PARENT, SEQUENCE, OPTCOUNTER) VALUES (" + (intMaxPxElement+2)  + ", "+promoName+", "+ (intMaxPxElement+2)+", 'PurchaseCondition', 'OrderLevelFixedShippingDiscountPurchaseCondition', null, 0.0, 0)";
                    insertPxElementSql2 = "INSERT INTO PX_ELEMENT (PX_ELEMENT_ID, PX_PROMOTION_ID, NAME, TYPE, SUBTYPE, PARENT, SEQUENCE, OPTCOUNTER) VALUES (" + (intMaxPxElement+3)  + ", "+promoName+", "+ (intMaxPxElement+3)+", 'IncludePaymentTypeIdentifier', 'Identifier_PaymentType', "+ (intMaxPxElement+2)+", 0.0, 0)";
                    insertPxElementSql3 = "INSERT INTO PX_ELEMENT (PX_ELEMENT_ID, PX_PROMOTION_ID, NAME, TYPE, SUBTYPE, PARENT, SEQUENCE, OPTCOUNTER) VALUES (" + (intMaxPxElement+4)  + ", "+promoName+", "+ (intMaxPxElement+4)+", 'AnyShipMode', 'Identifier_ShipMode', "+ (intMaxPxElement+2)+", 0.0, 0)";
                    
                    insertPxElementNVPSql1 = "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+1)  + ", " + (intMaxPxElement+2)  + ", 'Language', '44', 0)" ;
                    insertPxElementNVPSql2 = "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+2)  + ", " + (intMaxPxElement+2)  + ", 'MinimumPurchase', '1', 0)" ;
                    insertPxElementNVPSql3 = "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+3)  + ", " + (intMaxPxElement+2)  + ", 'AdjustmentType', 'IndividualAffectedItems', 0)" ;
                    insertPxElementNVPSql4 = "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+4)  + ", " + (intMaxPxElement+2)  + ", 'FixedCost', "+amountOff+", 0)" ;               
                    insertPxElementNVPSql5 = "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+5)  + ", " + (intMaxPxElement+2)  + ", 'Currency', 'GBP', 0)" ;
                    insertPxElementNVPSql6 = "INSERT INTO PX_ELEMENTNVP (PX_ELEMENTNVP_ID, PX_ELEMENT_ID, NAME, VALUE, OPTCOUNTER) VALUES (" + (intMaxPxElementNVP+6)  + ", " + (intMaxPxElement+3)  + ", 'PaymentType', 'Any', 0)" ;
                    
                    insertPxPromoAuthSql = "INSERT INTO PX_PROMOAUTH (PX_PROMOTION_ID, COMMENTS, PROMOTIONTYPE, DAILYSTARTTIME, DAILYENDTIME, WEEKDAY_SUN, WEEKDAY_MON, WEEKDAY_TUE, WEEKDAY_WED, WEEKDAY_THU, WEEKDAY_FRI, WEEKDAY_SAT, CTLPARAM, OPTCOUNTER, ADMINSTVENAME) VALUES ("+promoName+", 'null', 'OrderLevelFixedShippingDiscount', '2000-01-01 00:00:00', '2000-01-01 23:59:59', 1, 1, 1, 1, 1, 1, 1, 'CMC', 0,'" + promotionName + "')";
                    
                    insertPromoSql(insertPxElementSql);
                    insertPromoSql(insertPxElementSql1);
                    insertPromoSql(insertPxElementSql2);
                    insertPromoSql(insertPxElementSql3);
                                    
                    insertPromoSql(insertPxElementNVPSql1);
                    insertPromoSql(insertPxElementNVPSql2);
                    insertPromoSql(insertPxElementNVPSql3);
                    insertPromoSql(insertPxElementNVPSql4);
                    insertPromoSql(insertPxElementNVPSql5);                
                    insertPromoSql(insertPxElementNVPSql6);
                                  
                    insertPromoSql(insertPxPromoAuthSql);
                    updateStatus(promoId);
                   
                    getPromotionEtlHelper().commit();
                    getPromotionEtlHelper().close();                
                    archive(FILE_NAME,srcDirectory);                    
            }

        }

        catch (SQLException sql) {
            LOGGER.logp(Level.SEVERE, CLASSNAME, "main", "SQLException Exception" + sql.getMessage());

            System.exit(NO_SOURCE_FILE_FOUND);
        }

        LOGGER.logp(Level.INFO, CLASSNAME, "Creating insert scripts", "Exiting");

    }
        
        /**
         * @param sourceDirectory
         * @param promoId
         * @throws SQLException
         */
        private void updateStatus(String promoId) throws SQLException, ClassNotFoundException {      
            LOGGER.logp(Level.INFO, CLASSNAME, "main", "Updating XINT_PROMOTIONDATA table"); 
                  
            try{                              
                updateXINTPROMOTIONTable(promoId);            
                
            }catch(SQLException sql){
                LOGGER.logp(Level.SEVERE, CLASSNAME, "main", "SQLException Exception"+sql.getMessage());
                
                System.exit(NO_SOURCE_FILE_FOUND);        
            }

                LOGGER.logp(Level.INFO, CLASSNAME, "InsertParentProduct", "Exiting");
          
        }
        
        /**
         * This method created a promotion XML file from XINT_PROMOTIONDATA table
         * and will be used by the OOB utility to load the promotion.
         * 
         * @throws SQLException Exception
         */
        private void  updateXINTPROMOTIONTable(String promoId) throws SQLException, ClassNotFoundException {
            LOGGER.entering(CLASSNAME, "updateXINTPROMOTIONTable");
            PreparedStatement pstmt = null;
            PreparedStatement pstmt1 = null;
            int rs ;
            try {
                pstmt = getPromotionEtlHelper().getJdbcConnection().prepareStatement(PromotionEtlHelper.UPDATE_DATA_XINTPROMOTION_SQL);
                pstmt.setString(PromotionEtlHelper.PARAM_1, promoId);
                rs = pstmt.executeUpdate();

            } catch (SQLException sqle) {
                throw sqle;
            } finally {
                if (pstmt != null) {
                    pstmt.close();
                    pstmt = null;
                }
                if (pstmt1 != null) {
                    pstmt1.close();
                    pstmt1 = null;
                }
            }

        }        
        
        /**
         * Archive the source file to the archive folder. Delete the original source file.
         * 
         * @param sourceFileName - the source file name
         */
        private void archive(final String sourceFileName,String sourceDirectory) {
            LOGGER.entering(CLASSNAME, "archive");
            try {
                File sourceFile = new File(sourceDirectory + File.separator  + sourceFileName);               
                File archiveFile = new File(sourceDirectory + File.separator + ARCHIVE_FOLDER + File.separator + sourceFileName.replaceFirst("[.][^.]+$", "")+ "_" +new Date().getTime() + ".xml" );
                FileUtilities.copyFile(sourceFile, archiveFile);
                sourceFile.delete();
            } catch (IOException e) {
                logStackTrace(e, "archive");
                System.exit(ERROR_WRITING_ARCHIVE_FILE);
            }
            LOGGER.exiting(CLASSNAME, "archive");
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