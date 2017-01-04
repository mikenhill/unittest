package com.salmon.dataload.helper;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.naming.NamingException;

import com.salmon.dataload.helper.BaseJDBCHelper;

/**
 * MonitiseEtlHelper
 * @author Pranava Mishra
 *
 */
public class MonitiseEtlHelper extends BaseJDBCHelper {
    private static final String CLASSNAME = MonitiseEtlHelper.class.getName();
    private static final Logger LOGGER = Logger.getLogger(CLASSNAME);
    
    public static final int PARAM_1  = 1;
    public static final int PARAM_2  = 2;
    public static final int PARAM_3  = 3;
    public static final int PARAM_4  = 4;
    public static final int PARAM_5  = 5;
    public static final int PARAM_6  = 6;
    public static final int PARAM_7  = 7;
    public static final int PARAM_8  = 8;
    public static final int PARAM_9  = 9;
    public static final int PARAM_10 = 10;
    
    public static final int CPSKU = 3;
    TableName name;
    
    public static final String FIND_STOREENTID_SQL    = "SELECT STOREENT_ID FROM STOREENT WHERE IDENTIFIER =? WITH UR";
    public static final String FIND_FFMCENTERID_SQL   = "SELECT FFMCENTER_ID FROM FFMCENTER WHERE NAME= ? WITH UR";
    public static final String FIND_FILENAME_SQL      = "SELECT COUNT(*) FROM XETLFILENAME WHERE FILENAME LIKE ? WITH UR";
    public static final String FIND_STOREMEMBERID_SQL = "SELECT MEMBER_ID FROM STOREENT WHERE IDENTIFIER =? WITH UR";
    public static final String FIND_LANGUAGE_SQL      = "SELECT COUNT(*) FROM LANGUAGE WHERE LOCALENAME=? WITH UR";
    public static final String FIND_CURRENCY_SQL      = "SELECT COUNT(*) FROM CURLIST WHERE STOREENT_ID=? AND CURRSTR=? WITH UR"; 
    public static final String FIND_CATENTRY_SQL      = "SELECT COUNT(*) FROM CATENTRY WHERE PARTNUMBER=? AND CATENTTYPE_ID='ItemBean' WITH UR"; 
    public static final String FIND_ATTRIBUTE_SQL     = "SELECT COUNT(*) FROM ATTRVALDESC WHERE VALUE = ? AND ATTR_ID=(SELECT ATTR_ID FROM ATTR WHERE IDENTIFIER LIKE ?) WITH UR";
    public static final String FIND_XINT_CATEGORY_SQL = "SELECT attributevalue FROM xint_attributedata WHERE attributename=? AND sequence=? WITH UR";
    public static final String FIND_OFFER_STATUS_SQL  = "SELECT processed FROM xint_offersdata WHERE sequence = ? WITH UR";
    
    public static final String UPDATE_STOCKDATA_COMMENT_SQL="UPDATE XINT_STOCKDATA SET COMMENTS=?, PROCESSED=? WHERE XINT_STOCKDATA_ID IN";
    
    public static final String UPDATE_PRICEDATA_COMMENT_SQL="UPDATE XINT_PRICEDATA SET COMMENTS=?, PROCESSED=? WHERE XINT_PRICEDATA_ID IN";
    
    public static final String UPDATE_STOREDATA_COMMENT_SQL=" UPDATE XINT_STORESDATA SET COMMENTS=?, PROCESSED=? WHERE  XINT_STORESDATA_ID IN";
    
    public static final String UPDATE_MASDATA_COMMENT_SQL=" UPDATE XINT_MASDATA SET COMMENTS=?, PROCESSED=? WHERE  XINT_MASDATA_ID IN";
    
    public static final String UPDATE_ADDLANGDATA_COMMENT_SQL=" UPDATE XINT_ADDLANGDATA SET COMMENTS=?, PROCESSED=? WHERE  XINT_LANGDATA_ID IN ";
    
    public static final String UPDATE_ATTRIBUTEDATA_COMMENT_SQL=" UPDATE XINT_ATTRIBUTEDATA SET COMMENTS=?, PROCESSED=? WHERE SEQUENCE=? AND ATTRIBUTENAME =?";   
    
    public static final String UPDATE_OFFERSDATA_COMMENT_SQL=" UPDATE XINT_OFFERSDATA SET COMMENTS=?, PROCESSED=? WHERE SEQUENCE=? AND CPSKU=?";
  
    public static final String UPDATE_PROMOTIONDATA_COMMENT_SQL="UPDATE XINT_PROMOTIONDATA SET COMMENTS=?, PROCESSED=? WHERE  XINT_PROMODATA_ID IN";
    
    public static final String LOG_STOCKDATA_SQL     = "SELECT CPCODE, CPSKU, QUANTITY, ERRORDESCRIPTION(COMMENTS) COMMENTS FROM XINT_STOCKDATA WHERE INPUT_FILENAME=? WITH UR";
    public static final String LOG_PRICEDATA_SQL     = "SELECT CPCODE, CPPRICE, CPSKU, CURRENCYCODE, OFFER_FROMDATE, OFFER_TODATE, ERRORDESCRIPTION(COMMENTS) COMMENTS FROM XINT_PRICEDATA WHERE INPUT_FILENAME=? WITH UR";
    public static final String LOG_STORESDATA_SQL    = "SELECT CPCODE, CPSTORECODE, NAME, ADDRESS1, ADDRESS2, ADDRESS3, CITY, STATE, COUNTRY, POSTCODE, PHONE, FAX, ACTIVE, LATITUDE, LONGITUDE, IMAGE1, ERRORDESCRIPTION(COMMENTS) COMMENTS FROM XINT_STORESDATA WHERE INPUT_FILENAME=? WITH UR";
    public static final String LOG_MASDATA_SQL       = "SELECT CPCODE, CPSOURCESKU, CPTARGETSKU, MATYPE, INPUT_FILENAME, PROCESSED, LASTUPDATED, ERRORDESCRIPTION(COMMENTS) COMMENTS FROM XINT_MASDATA WHERE INPUT_FILENAME=? WITH UR";
    public static final String LOG_OFFERSDATA_SQL    = "SELECT CPCODE, CPSKU, CPPARENTSKU, NAME, PRODUCTTYPE, SHORTDESCRIPTION, LONGDESCRIPTION, LISTPRICE, OFFERPRICE, CURRENCY, ONSPECIAL, PHYSICALSTOCK, LANGUAGE, INVENTORY, IMAGE1, IMAGE2, IMAGE3, IMAGE4, STARTDATE, ENDDATE, AGERESTRICTION, BRAND, TARGETEDLOCATION, TARGETEDPROXIMITY, TARGETEDPOPULATION, KEYWORD,  ERRORDESCRIPTION(COMMENTS) COMMENTS FROM XINT_OFFERSDATA WHERE INPUT_FILENAME=? WITH UR";
    public static final String LOG_LANGDATA_SQL      = "SELECT CPCODE, CPSKU, LANGUAGE, NAME, SHORTDESCRIPTION, LONGDESCRIPTION,  ERRORDESCRIPTION(COMMENTS) COMMENTS FROM XINT_ADDLANGDATA WHERE INPUT_FILENAME=? WITH UR";
    public static final String LOG_ATTRIBUTEDATA_SQL = "SELECT CPCODE, CPSKU, PRODUCTTYPE, ATTRIBUTENAME, ATTRIBUTEVALUE, ERRORDESCRIPTION(COMMENTS) COMMENTS FROM XINT_ATTRIBUTEDATA WHERE INPUT_FILENAME=? WITH UR";
    public static final String LOG_PROMOTIONDATA_SQL = "SELECT CPCODE, CPSKU, PRODUCTTYPE, PERCENTAGEOFF, AMOUNTOFF, SHIPQUALAMOUNT, ERRORDESCRIPTION(COMMENTS) COMMENTS FROM XINT_PROMOTIONDATA WHERE INPUT_FILENAME=? WITH UR";
    public static final String LOG_OFFER_ATTRIBUTE_DATA_SQL = "SELECT  XO.CPCODE, XO.CPSKU, XO.CPPARENTSKU, XO.NAME, XO.PRODUCTTYPE," 
                                                            + " XO.SHORTDESCRIPTION, XO.LONGDESCRIPTION, XO.LISTPRICE, XO.OFFERPRICE, XO.CURRENCY," 
                                                            + " XO.ONSPECIAL, XO.PHYSICALSTOCK, XO.LANGUAGE, "
                                                            + " XO.INVENTORY, XO.IMAGE1, XO.IMAGE2, XO.IMAGE3, XO.IMAGE4, XO.STARTDATE," 
                                                            + " XO.ENDDATE, XO.AGERESTRICTION, XO.BRAND, XO.TARGETEDLOCATION "
                                                            + " ,XO.TARGETEDPROXIMITY, XO.TARGETEDPOPULATION, XO.KEYWORD, XA.ATTRIBUTENAME, XA.ATTRIBUTEVALUE,  ERRORDESCRIPTION(XO.COMMENTS||'|'||XA.COMMENTS) COMMENTS "  
                                                            + " FROM XINT_OFFERSDATA XO, XINT_ATTRIBUTEDATA XA WHERE SUBSTR(XO.INPUT_FILENAME,8,19)=SUBSTR(XA.INPUT_FILENAME,11,19) AND XO.CPCODE=XA.CPCODE  AND XO.CPSKU=XA.CPSKU " 
                                                            + " AND XO.PRODUCTTYPE= XA.PRODUCTTYPE  AND XA.INPUT_FILENAME=?   ORDER BY 1, 2, 3, 4, 5 WITH UR";
    
    public static final String UPDATE_OFFERS_PROCESSED_FLAG_FAILED_SQL        = "UPDATE XINT_OFFERSDATA SET PROCESSED=2 WHERE SEQUENCE=?";
    public static final String UPDATE_PARENT_OFFERS_PROCESSED_FLAG_FAILED_SQL = "UPDATE XINT_OFFERSDATA SET PROCESSED=2 WHERE INPUT_FILENAME = ? AND CPSKU IN (SELECT CPSKU FROM XINT_OFFERSDATA WHERE CPPARENTSKU=? OR CPSKU=?)";
    public static final String UPDATE_ATTRIBUTES_PROCESSED_FLAG_FAILED_SQL    = "UPDATE XINT_ATTRIBUTEDATA SET PROCESSED=2 WHERE SEQUENCE=?";
    public static final String FIND_PARENT_PRODUCT_ATTRS_SQL                  = "SELECT INPUT_FILENAME, CPPARENTSKU FROM XINT_OFFERSDATA WHERE SEQUENCE = ? WITH UR";
    
    
    public static final String UPDATE_STOCK_PROCESSED_FLAG_SQL     = "UPDATE XINT_STOCKDATA SET PROCESSED=3 WHERE CPSKU=? AND CPCODE=? AND QUANTITY=?";
    public static final String UPDATE_PRICE_PROCESSED_FLAG_SQL     = "UPDATE XINT_PRICEDATA SET PROCESSED=3 WHERE CPSKU=? AND OFFER_FROMDATE=? AND OFFER_TODATE=? AND CPPRICE=?";
    public static final String UPDATE_STORES_PROCESSED_FLAG_SQL    = "UPDATE XINT_STORESDATA SET PROCESSED=3 WHERE CPCODE=? AND CPSTORECODE=? AND NAME=?";
    public static final String UPDATE_MAS_PROCESSED_FLAG_SQL       = "UPDATE XINT_MASDATA SET PROCESSED=3 WHERE CPSOURCESKU=? AND MATYPE=? AND CPTARGETSKU=?";
    public static final String UPDATE_OFFERS_PROCESSED_FLAG_SQL    = "UPDATE XINT_OFFERSDATA SET PROCESSED=3 WHERE SEQUENCE=?";
    public static final String UPDATE_ADDLANG_PROCESSED_FLAG_SQL   = "UPDATE XINT_ADDLANGDATA SET PROCESSED=3 WHERE CPSKU=? AND LANGUAGE=? AND NAME=?";
    public static final String UPDATE_ATTRIBUTE_PROCESSED_FLAG_SQL = "UPDATE XINT_ATTRIBUTEDATA SET PROCESSED=3 WHERE SEQUENCE=? AND PROCESSED=1";
    public static final String UPDATE_PROMOTION_PROCESSED_FLAG_SQL = "UPDATE XINT_PROMOTIONDATA SET PROCESSED=3 WHERE PROCESSED=0";
    
    public static final String FIND_PRODUCTS_WITHOUT_ATTRIBUTES_SQL = "SELECT DISTINCT O.CPSKU, O.CPCODE " +
                                                                      "  FROM XINT_OFFERSDATA O " +
                                                                      " WHERE O.CPPARENTSKU IS NULL " +
                                                                      "   AND O.PROCESSED   = 1 ";
        
    public static final String FIND_ITEM_ATTRIBUTE_SQL = "SELECT A.CPCODE, O.CPSKU, A.PRODUCTTYPE, A.ATTRIBUTENAME, A.ATTRIBUTEVALUE, O.SEQUENCE, A.INPUT_FILENAME, A.LASTUPDATED " + 
                                                         "  FROM XINT_ATTRIBUTEDATA A, XINT_OFFERSDATA O " +
                                                         " WHERE O.CPSKU       = ? " +
                                                         "   AND O.CPPARENTSKU IS NULL " +
                                                         "   AND O.PROCESSED   = 1 " +
                                                         "   AND A.SEQUENCE    IN (SELECT MIN(SEQUENCE) " +
                                                         "                           FROM XINT_OFFERSDATA " +
                                                         "                          WHERE CPPARENTSKU = ? " +
                                                         "                            AND CPCODE      = ? " +
                                                         "                            AND PROCESSED   = 1)";
    
    public static final String INSERT_PRODUCT_ATTRIBUTE_SQL = "INSERT INTO XINT_ATTRIBUTEDATA ( XINT_ATTRDATA_ID, CPCODE, CPSKU, PRODUCTTYPE, ATTRIBUTENAME, ATTRIBUTEVALUE, SEQUENCE, INPUT_FILENAME, PROCESSED, LASTUPDATED) " +
                                                    "VALUES((SELECT MAX(XINT_ATTRDATA_ID)+1 FROM XINT_ATTRIBUTEDATA), ?, ?, ?, ?, ?, ?, ?, '1', ?)";
    
    
    public static final String FIND_NON_EXISTENT_PRODUCTS_SQL = "SELECT DISTINCT X1.CPPARENTSKU, X1.CPCODE " + 
    "  FROM XINT_OFFERSDATA X1 " +
    " WHERE X1.PROCESSED   = 1 " +
    "   AND X1.CPPARENTSKU IS NOT NULL " +
    "   AND NOT EXISTS (SELECT 1 " + 
    "                     FROM XINT_OFFERSDATA X2  " + 
    "                    WHERE X2.CPSKU     = X1.CPPARENTSKU " +
    "                      AND X2.PROCESSED = 1) WITH UR ";       
    
    
    
    
    public static final String CREATE_PARENT_PRODUCTS_SQL =
        "INSERT INTO XINT_OFFERSDATA ( " +
                                     "XINT_OFFERSDATA_ID, " +
                                     "CPCODE, CPSKU, CPPARENTSKU, NAME, PRODUCTTYPE, SHORTDESCRIPTION, LONGDESCRIPTION, " +
                                     "LISTPRICE, OFFERPRICE, CURRENCY, ONSPECIAL, PHYSICALSTOCK, LANGUAGE, INVENTORY, IMAGE1, IMAGE2, IMAGE3, IMAGE4, " +
                                     "STARTDATE, ENDDATE, AGERESTRICTION, BRAND, TARGETEDLOCATION, TARGETEDPROXIMITY, TARGETEDPOPULATION, " +
                                     "KEYWORD, SEQUENCE, " +
                                     "INPUT_FILENAME, PROCESSED, LASTUPDATED, COMMENTS " + 
                                     ")"+
                             "SELECT DISTINCT (SELECT MAX(XINT_OFFERSDATA_ID) + 1 FROM XINT_OFFERSDATA WITH UR) AS XINT_OFFERSDATA_ID , " + 
                                     "CPCODE, CPPARENTSKU, NULL, NAME, PRODUCTTYPE, SHORTDESCRIPTION, LONGDESCRIPTION, " +
                                     "LISTPRICE, OFFERPRICE, CURRENCY, ONSPECIAL, PHYSICALSTOCK, LANGUAGE, INVENTORY, IMAGE1, IMAGE2, IMAGE3, IMAGE4, " +
                                     "STARTDATE, ENDDATE, AGERESTRICTION, BRAND, TARGETEDLOCATION, TARGETEDPROXIMITY, TARGETEDPOPULATION, " + 
                                     "KEYWORD, (SELECT MAX(SEQUENCE) + 1 FROM XINT_OFFERSDATA) AS SEQUENCE ," +
                                     "INPUT_FILENAME, PROCESSED, LASTUPDATED, COMMENTS " +
                               "FROM XINT_OFFERSDATA " + 
                              "WHERE CPPARENTSKU        = ? " + 
                              "  AND XINT_OFFERSDATA_ID = (SELECT MIN(XINT_OFFERSDATA_ID) " +
                              "                              FROM XINT_OFFERSDATA " + 
                              "                             WHERE CPPARENTSKU = ? " + 
                              "                               AND CPCODE      = ? " +
                              "                               AND PROCESSED   = 1 WITH UR) WITH UR";
    
    public static final String FIND_CATALOG_IDENTIFIER_SQL ="SELECT IDENTIFIER FROM CATALOG WHERE CATALOG_ID = (SELECT CATALOG_ID FROM STORECAT WHERE STOREENT_ID = (SELECT STOREENT_ID FROM STOREENT WHERE IDENTIFIER=?))";

    public static final String FIND_ATTRIBUTE_VALUE_SQL ="SELECT value FROM attrvaldesc WHERE attr_id=(SELECT attr_id FROM attr WHERE identifier =?) WITH UR";
    public static final String FIND_MAX_SEQUENCE_SQL ="SELECT MAX(SEQUENCE)+1 FROM XINT_OFFERSDATA";
    public static final String FIND_ALL_PRODUCT_ATTRIBUTES_SQL ="SELECT ATTRIBUTENAME FROM XINT_ATTRIBUTEDATA WHERE SEQUENCE =? AND ATTRIBUTEVALUE IS NOT NULL";
    public static final String FIND_PROCESSED_FLAG_SQL ="SELECT PROCESSED FROM XINT_ATTRIBUTEDATA WHERE SEQUENCE =? AND ATTRIBUTENAME=?";
           
    public MonitiseEtlHelper(TableName name)  {
           this.name=name;
    }
    
    /**
     * MonitiseEtlHelper
     * @param jdbcDriver String
     * @param jdbcUrl String
     * @param dbUser String
     * @param dbPassword String
     * @throws Exception Exception
     */
    
    public MonitiseEtlHelper(final String jdbcDriver, final String jdbcUrl, 
            final String dbUser, final String dbPassword) throws ClassNotFoundException, SQLException {
           makeConnection(jdbcDriver, jdbcUrl, dbUser, dbPassword);
    }
    
    /**
     * @param input_filename
     * @return true or false
     * @throws NamingException
     * @throws SQLException
     * @throws ClassNotFoundException 
     */
    
    public boolean fileExists(String input_filename)throws NamingException, SQLException
        {
        LOGGER.entering(CLASSNAME, "fileExists", " check file exists in db for file name - "+input_filename);
           int result = 0;
           ResultSet rs = null;
           PreparedStatement pstmt = null;
           try{
               pstmt = getJdbcConnection().prepareStatement(FIND_FILENAME_SQL);
               pstmt.setString(PARAM_1, input_filename);           
               rs = pstmt.executeQuery();
               if (rs.next()) {
                   result = Integer.parseInt(rs.getString(1));              
               }
           } catch (SQLException sqle) {           
                   throw sqle;           
           } finally {
               if(rs!=null){rs.close();rs=null;}
               if(pstmt!=null){pstmt.close();pstmt=null;}
           }
           
           LOGGER.exiting(CLASSNAME, "fileExists", "Source File Exists result in db:" + result);
           return result > 0 ;     
      }
    
    /* 
     * This method checks if the content provider code provides is valid or not. If not returns an errorCode. 
     */
    public String isValidCpCode(String cpCode)throws NamingException, SQLException{
        String methodName = "isValidCpCode";
        LOGGER.entering(CLASSNAME, methodName, "CP Code:" + cpCode);        

        String errorCode  = "";
        
        if (cpCode == null ||
            cpCode.trim().length() == 0 ||
            cpCode.length() > 64) {        
                
            errorCode="E024";        
        
        } else { 
        
            int    countStore = 0;
            String memberId   = null;
            
            ResultSet         rs    = null;
            PreparedStatement pstmt = null;
            
            try{
                pstmt = getJdbcConnection().prepareStatement(FIND_STOREMEMBERID_SQL);
                pstmt.setString(PARAM_1, cpCode);
                
                rs = pstmt.executeQuery();
                if(rs.next()){
                    memberId = rs.getString(1);  
                    countStore=1;
                    LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Valid store found");        
                }            
            } catch (SQLException sqle) {           
                throw sqle;           
            } finally {
                if(rs!=null){rs.close();rs=null;}
                if(pstmt!=null){pstmt.close();pstmt=null;}
            }        
            
            if(countStore==0){
                errorCode="E002";
                LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Invalid CP - E002");    
            }
        }
        LOGGER.exiting(CLASSNAME, methodName);
        return errorCode;
    }
    
    /* 
     * This method checks if a valid FFMCenterId exist corresponding to the CP Code. If not returns an errorCode. 
     */
    public String findFFMCenterId(String cpCode)throws NamingException, SQLException{
        LOGGER.entering(CLASSNAME, "findFFMCenterId", " check if the FFMCenterId is valid - "+cpCode);        
        String ffmCenterId="";
        ResultSet rs = null;        
        PreparedStatement pstmt = null;
        try{
            pstmt = getJdbcConnection().prepareStatement(FIND_FFMCENTERID_SQL);
            pstmt.setString(PARAM_1, cpCode);
            rs = pstmt.executeQuery();
            if(rs.next()){
                ffmCenterId = (rs.getString(1)!=null?rs.getString(1):""); 
            }
        } catch (SQLException sqle) {           
            throw sqle;           
        } finally {
            if(rs!=null){rs.close();rs=null;}
            if(pstmt!=null){pstmt.close();pstmt=null;}
        }   
        
        if(ffmCenterId=="")
        {
            ffmCenterId="E003";
        }        
        LOGGER.exiting(CLASSNAME, "findFFMCenterId", "errorCode if the valid ffmCenter is not found:");
        return ffmCenterId;
    }

    /* 
     * This method checks if a valid Storeent exist corresponding to the CP Code. If not returns an errorCode. 
     */
    public String findStoreentId(String cpCode)throws NamingException, SQLException{
        LOGGER.entering(CLASSNAME, "findStoreentId", " check if the storeentId is valid - "+cpCode);        
        String storeentId="";
        ResultSet rs = null;        
        PreparedStatement pstmt = null;
        try{
            pstmt = getJdbcConnection().prepareStatement(FIND_STOREENTID_SQL);
            pstmt.setString(PARAM_1, cpCode);
            rs = pstmt.executeQuery();
            if(rs.next()){
                storeentId = (rs.getString(1)!=null?rs.getString(1):""); 
            }   
        } catch (SQLException sqle) {           
            throw sqle;           
        } finally {
            if(rs!=null){rs.close();rs=null;}
            if(pstmt!=null){pstmt.close();pstmt=null;}
        }        
        if(storeentId=="")
        {
            storeentId="E002";
        }        
        LOGGER.exiting(CLASSNAME, "findStoreentId", "errorCode if the valid storeentId is not found:");
        return storeentId;
    }
    
    /* 
     * This method checks if a valid currency exist corresponding to the store. If not returns an errorCode. 
     */
    public String isValidCurrencyCode(String currencyCode, int storeentId)throws NamingException, SQLException{
        LOGGER.entering(CLASSNAME, "isValidCurrencyCode", " check if the currency is valid - "+currencyCode +storeentId);
        String errorCode=null;
        int countCurrency=0;
        ResultSet rs = null;        
        PreparedStatement pstmt = null;
        try{
            pstmt = getJdbcConnection().prepareStatement(FIND_CURRENCY_SQL);
            pstmt.setInt   (PARAM_1, storeentId);
            pstmt.setString(PARAM_2, currencyCode);
            rs = pstmt.executeQuery();
            if(rs.next()){
                countCurrency = rs.getInt(1); 
            }
        } catch (SQLException sqle) {           
            throw sqle;           
        } finally {
            if(rs!=null){rs.close();rs=null;}
            if(pstmt!=null){pstmt.close();pstmt=null;}
        }            
        if(countCurrency == 0)
        {
            errorCode="E011";
        }        
        LOGGER.exiting(CLASSNAME, "isValidCurrencyCode", "errorCode if the valid currency is not found:" + errorCode);
        return errorCode;
    }
    
    /* 
     * This method checks if the string provided is a valid date-time format. If not returns an errorCode. 
     */
    public String isValidAttributeDate(final String date, final String attributeName){
        String methodName = "isValidAttributeDate";
        LOGGER.entering(CLASSNAME, methodName, "Date:" + date + " attributeName:" + attributeName);
        String errorCode=isValidDate(date);
        if (errorCode != null) {
            errorCode = errorCode + ":" + attributeName;
        }       
        LOGGER.exiting(CLASSNAME, methodName);
        return errorCode;
    }
    
    /* 
     * This method checks if the string provided is a valid date-time format. If not returns an errorCode. 
     */
    public String isValidDate(final String date){
        String methodName = "isValidDate";
        LOGGER.entering(CLASSNAME, methodName, "check if the supplied date is valid:" + date);
        String errorCode=null;
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            sdf.setLenient(false);
            sdf.parse(date);
          }
          catch (ParseException e) {
              LOGGER.logp(Level.INFO, CLASSNAME, methodName, "exception caught while validating date:" + date);
              errorCode="E007";
          }
          catch (IllegalArgumentException e) {
              LOGGER.logp(Level.INFO, CLASSNAME, methodName, "exception caught while validating date:" + date);
              errorCode="E007";
          }
          LOGGER.exiting(CLASSNAME, methodName);
          return errorCode;
    }
    
    /* 
     * This method performs validation of the fromDate and the toDate. If not returns an errorCode. 
     */
    public String isValidFromAndToDates(String toDate,String fromDate){
        LOGGER.entering(CLASSNAME, "isValidFromAndToDates", "From date:" + fromDate + " To date:" + toDate);
        String errorCode=isValidDate(toDate);
        if((toDate!=null && toDate.length()>0) && (fromDate==null || fromDate.length()==0)){
            errorCode=errorCode!=null?errorCode.concat("|E008"):"E008";
        }
        try{
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date fDate = sdf.parse(fromDate);
            Date tDate = sdf.parse(toDate);  
            long d1Ms=fDate.getTime();
            long d2Ms=tDate.getTime();
            long diff=(d2Ms-d1Ms)/60000;
            LOGGER.logp(Level.INFO, CLASSNAME, "isValidFromAndToDates", " Difference between from and to date - :::"+diff );
            if(diff<0){            
                errorCode=errorCode!=null?errorCode.concat("|E009"):"E009";
            }
        }
        catch (ParseException e) {
            LOGGER.exiting(CLASSNAME, "isValidFromAndToDates", "exception caught while validating date:" + fromDate + toDate);
        }        
        return errorCode;
    }
    /* 
     * This method checks if the value provided is a valid integer and greater than 0. 
     * If not returns an errorCode along with the attribute name. 
     */
    public String isValidInteger(String value, String attributeName){
        String methodName = "isValidInteger";
        LOGGER.entering(CLASSNAME, methodName, " check if the attribute is valid integer- "+value );
        String errorCode=null;
        long isValidInteger=0;
        try{
            isValidInteger=Long.parseLong(value);
            if (isValidInteger < 0) {
                errorCode="E006:"+attributeName;
            }
        }
        catch(NumberFormatException e){
            LOGGER.logp(Level.INFO, CLASSNAME, methodName, " cannot convert String to integer - "+value );
            errorCode="E006:"+attributeName;
        }        
        LOGGER.exiting(CLASSNAME, methodName, " error code if the value is not valid integer "+errorCode );
        return errorCode ;
    }
    /* 
     * This method checks if the language is valid or not. If not returns an errorCode. 
     */
    public String isValidLanguage(String language)throws NamingException, SQLException{
        LOGGER.entering(CLASSNAME, "isValidLanguage", " check if the language is valid - "+language );
        String errorCode=null;
        int countLanguage=0;
        ResultSet rs = null;        
        PreparedStatement pstmt = null;
        try{
            pstmt = getJdbcConnection().prepareStatement(FIND_LANGUAGE_SQL);
            pstmt.setString(PARAM_1, language.length()>10?language.substring(0, 10):language);
            rs = pstmt.executeQuery();
            if(rs.next()){
                countLanguage=rs.getInt(1);
                LOGGER.logp(Level.INFO, CLASSNAME, "isValidLanguage", " language is valid - "+language );
            }
        } catch (SQLException sqle) {           
            throw sqle;           
        } finally {
            if(rs!=null){rs.close();rs=null;}
            if(pstmt!=null){pstmt.close();pstmt=null;}
        }   
        
        if(countLanguage == 0)
        {
            errorCode="E013";
        }
        LOGGER.exiting(CLASSNAME, "isValidLanguage", " error code if the language is not valid "+errorCode );
        return errorCode;
    }
    /* 
     * This method checks if the provided value is a valid integer greater than 0. 
     * If not returns an errorCode along with the attribute name. 
     */
    public String isValidNumericValue(final String value, final String attributeName){
        String methodName = "isValidNumericValue";
        LOGGER.entering(CLASSNAME, methodName, " check if the attribute is valid integer- "+value );
        String errorCode=null;
        double isValidNumber=0.0;
        try{
            if(value.contains(".")){
                isValidNumber=Double.parseDouble(value);
            }   else{
                isValidNumber=Long.parseLong(value);
            }
            if (isValidNumber<0){
                errorCode="E010:"+attributeName;
            }
        }        
        catch(NumberFormatException e){
            LOGGER.logp(Level.INFO, CLASSNAME, methodName, " Provided Value is not a valid number- "+value );
            errorCode="E010:"+attributeName;
        }        
        LOGGER.exiting(CLASSNAME, methodName, " error code if the language is not valid "+errorCode );
        return errorCode;
    }  
    
    /* 
     * This method checks if the provided value is a valid integer greater than 0. 
     * If not returns an errorCode along with the attribute name. 
     */
    public String isPromotionValidNumericValue(final String value, final String attributeName){
        String methodName = "isValidNumericValue";
        LOGGER.entering(CLASSNAME, methodName, " check if the attribute is valid integer- "+value );
        String errorCode=null;
        double isValidNumber=0.0;
        try{
            if (!value.equals("")){
            if(value.contains(".")){
                isValidNumber=Double.parseDouble(value);
            }   else{
                isValidNumber=Long.parseLong(value);
            }
            if (isValidNumber<0){
                errorCode="E010:"+attributeName;
            }
            }
        }        
        catch(NumberFormatException e){
            LOGGER.logp(Level.INFO, CLASSNAME, methodName, " Provided Value is not a valid number- "+value );
            errorCode="E010:"+attributeName;
        }        
        LOGGER.exiting(CLASSNAME, methodName, " error code if the language is not valid "+errorCode );
        return errorCode;
    }  

    /* 
     * This method checks to see if that the product type is entered and doesn't contains more than 254 characters - E018
     */
    public String isValidProductType(final String productType)throws NamingException, SQLException{
        String methodName = "isValidProductType";
        LOGGER.entering(CLASSNAME, methodName, "productType:" + productType);
        String errorCode=null;
        
        if (productType == null ||
            productType.trim().length() == 0 ||
            productType.length() > 254) {        
            errorCode="E022:" + "product type";        
        } else {
            errorCode = isValidPreDefinedValue(productType, "productType");
        }
        
        LOGGER.exiting(CLASSNAME, methodName, "errorCode:" + errorCode);       
        return errorCode;
    }
    
    /* 
     * This method checks attribute name is already defined in the attribute dictionary. If not returns an errorCode. 
     */
    public String isValidPreDefinedValue(final String attributeValue, final String attributeName)throws NamingException, SQLException{
        String methodName = "isValidPreDefinedValue";
        LOGGER.entering(CLASSNAME, methodName, " Attribute name:" + attributeName + " Attribute value:" + attributeValue);
        String errorCode=null;
        int countAttribute=0;
        
        ResultSet         rs    = null;        
        PreparedStatement pstmt = null;
        try{
            pstmt = getJdbcConnection().prepareStatement(FIND_ATTRIBUTE_SQL);
            pstmt.setString(PARAM_1, attributeValue);
            pstmt.setString(PARAM_2, attributeName);        
            rs = pstmt.executeQuery();
            if(rs.next()){
                countAttribute = rs.getInt(1);             
            }  
        } catch (SQLException sqle) {           
            throw sqle;           
        } finally {
            if(rs!=null){rs.close();rs=null;}
            if(pstmt!=null){pstmt.close();pstmt=null;}
        }
                
        if(countAttribute == 0) {
            errorCode="E001" + "(" + attributeName + ")";
            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Error code: " + errorCode);
        }
        LOGGER.exiting(CLASSNAME, methodName, "errorCode if the attribute does exist" + errorCode);
        return errorCode;
    }
    /* 
     * This method checks to see if the sku contains more than 64 characters - E018
     * This method checks if the SKU already exists. If not returns an errorCode. 
     */
    public String isValidSku(final String sku, final String attributeName)throws NamingException, SQLException{
        LOGGER.entering(CLASSNAME, "isValidSku", "Sku:" + sku + " attributeName:" + attributeName);
        
        String errorCode=isValidSkuSize(sku, attributeName);
        
        if (errorCode == null) { 
        
            int               countSKU = 0;
            ResultSet         rs       = null;        
            PreparedStatement pstmt    = null;
            try{
                pstmt = getJdbcConnection().prepareStatement(FIND_CATENTRY_SQL);
                pstmt.setString(PARAM_1, sku);
                rs = pstmt.executeQuery();
                if(rs.next()){
                    countSKU = Integer.parseInt(rs.getString(1));             
                }
            } catch (SQLException sqle) {           
                throw sqle;           
            } finally {
                if(rs!=null){rs.close();rs=null;}
                if(pstmt!=null){pstmt.close();pstmt=null;}
            } 

            if(countSKU == 0) {
                errorCode="E004:" + attributeName; 
            }  
        }
        LOGGER.exiting(CLASSNAME, "isValidSku", "errorCode:" + errorCode);       
        return errorCode;
    }

    /* 
     * This method checks to see if that the sku is entered and doesn't contains more than 64 characters - E018
     */
    public String isValidSkuSize(final String sku, final String attributeName)throws NamingException, SQLException{
        LOGGER.entering(CLASSNAME, "isValidSku", "Sku:" + sku + " attributeName:" + attributeName);
        String errorCode=null;
        
        if (sku == null ||
            sku.trim().length() == 0 ||
            sku.length() > 64) {        
            errorCode="E018:" + attributeName;        
        } 
        LOGGER.exiting(CLASSNAME, "isValidSku", "errorCode:" + errorCode);       
        return errorCode;
    }
    
    /* 
     * This method checks for a null or empty SKU name. If not returns an errorCode. 
     */
    public String isValidSkuName(final String name) {
        String methodName = "isValidSkuName";
        LOGGER.entering(CLASSNAME, methodName, "name:" + name);
        
        String errorCode=null;
        
        if(name==null || name.trim().length()==0){
            errorCode="E012";
            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Invalid Sku Name - E012"); 
        } else if (name.length() > 128) {
            errorCode="E017";
            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Invalid Sku Name - E017"); 
        }
        LOGGER.exiting(CLASSNAME, methodName, "ErrorCode:" + errorCode);
        return errorCode;
    }
    
    /* This method checks for a null or empty storecode. If  not returns an errorCode. */
    public String isValidStoreCode(String storeCode){
        String errorCode=null;
        if(storeCode==null || storeCode.length()==0){
            errorCode="E014";
        }
        return errorCode;
    }
    /* 
     * This method checks for a null or empty storename.If  not returns an errorCode. 
     */
    public String isValidStoreName(String name){
        String errorCode=null;
        if(name==null || name.length()==0){
            errorCode="E015";
        }
        return errorCode;
    }
   
    
    /*              
     * This method update all xint comment column for error code 
     */
    
    public void updateErrorCode(TableName tableName, String[]  rowdata, String comment, String status) throws SQLException{
        String methodName = "updateErrorCode";
        LOGGER.entering(CLASSNAME, methodName); 
        LOGGER.logp(Level.INFO, CLASSNAME, methodName, "tableName:" + tableName + " comment:" + comment + " status:" + status);               
        PreparedStatement pstmt = null;
        StringBuffer paramSQL=new StringBuffer();
        try{
                switch(tableName){
                    case XINT_STOCKDATA:
                        paramSQL.append("(SELECT XINT_STOCKDATA_ID FROM XINT_STOCKDATA WHERE 1=1 ") ;
                        if(rowdata.length > 0 && rowdata[0]!=null  && !rowdata[0].equals(""))paramSQL.append(" AND CPCODE = '"+rowdata[0]+"'");else paramSQL.append(" AND CPCODE IS NULL");
                        if(rowdata.length > 1 && rowdata[1]!=null  && !rowdata[1].equals(""))paramSQL.append(" AND CPSKU = '"+rowdata[1]+"'");else paramSQL.append(" AND CPSKU IS NULL");
                        if(rowdata.length > 2 && rowdata[2]!=null  && !rowdata[2].equals(""))paramSQL.append(" AND QUANTITY = '"+rowdata[2]+"'");else paramSQL.append(" AND QUANTITY IS NULL");
                        if(rowdata.length > 3 && rowdata[3]!=null  && !rowdata[3].equals(""))paramSQL.append(" AND INPUT_FILENAME = '"+rowdata[3]+"'");else paramSQL.append(" AND INPUT_FILENAME IS NULL");
                        paramSQL.append(")");
                        pstmt = getJdbcConnection().prepareStatement(UPDATE_STOCKDATA_COMMENT_SQL+paramSQL.toString());
                        break;
                    case XINT_PRICEDATA:
                        paramSQL.append("(SELECT XINT_PRICEDATA_ID FROM XINT_PRICEDATA WHERE 1=1 ");
                        if(rowdata.length > 0 && rowdata[0]!=null  && !rowdata[0].equals(""))paramSQL.append(" AND CPCODE = '"+rowdata[0]+"'");else paramSQL.append(" AND CPCODE IS NULL");    
                        if(rowdata.length > 1 && rowdata[1]!=null  && !rowdata[1].equals(""))paramSQL.append(" AND CPSKU = '"+rowdata[1]+"'");else paramSQL.append(" AND CPSKU IS NULL");
                        if(rowdata.length > 2 && rowdata[2]!=null  && !rowdata[2].equals(""))paramSQL.append(" AND OFFER_FROMDATE = '"+rowdata[2]+"'");else paramSQL.append(" AND OFFER_FROMDATE IS NULL");
                        if(rowdata.length > 3 && rowdata[3]!=null  && !rowdata[3].equals(""))paramSQL.append(" AND OFFER_TODATE = '"+rowdata[3]+"'");else paramSQL.append(" AND OFFER_TODATE IS NULL");
                        if(rowdata.length > 4 && rowdata[4]!=null  && !rowdata[4].equals(""))paramSQL.append(" AND CPPRICE = '"+rowdata[4]+"'");else paramSQL.append(" AND CPPRICE IS NULL");
                        if(rowdata.length > 5 && rowdata[5]!=null  && !rowdata[5].equals(""))paramSQL.append(" AND CURRENCYCODE= '"+rowdata[5]+"'");else paramSQL.append(" AND CURRENCYCODE IS NULL");
                        if(rowdata.length > 6 && rowdata[6]!=null  && !rowdata[6].equals(""))paramSQL.append(" AND INPUT_FILENAME= '"+rowdata[6]+"'");else paramSQL.append(" AND INPUT_FILENAME IS NULL");
                        paramSQL.append(")");
                        pstmt = getJdbcConnection().prepareStatement(UPDATE_PRICEDATA_COMMENT_SQL+paramSQL.toString());                
                        break;
                    case XINT_STORESDATA:                        
                        paramSQL.append("(SELECT XINT_STORESDATA_ID FROM XINT_STORESDATA WHERE  1=1 ");
                        if(rowdata.length > 0 && rowdata[0]!=null  && !rowdata[0].equals(""))paramSQL.append(" AND CPCODE = '"+rowdata[0]+"'");else paramSQL.append(" AND CPCODE IS NULL");    
                        if(rowdata.length > 1 && rowdata[1]!=null  && !rowdata[1].equals(""))paramSQL.append(" AND CPSTORECODE = '"+rowdata[1]+"'");else paramSQL.append(" AND CPSTORECODE IS NULL");
                        if(rowdata.length > 2 && rowdata[2]!=null  && !rowdata[2].equals(""))paramSQL.append(" AND NAME = '"+rowdata[2]+"'");else paramSQL.append(" AND NAME IS NULL");
                        if(rowdata.length > 3 && rowdata[3]!=null  && !rowdata[3].equals(""))paramSQL.append(" AND ADDRESS1 = '"+rowdata[3]+"'");else paramSQL.append(" AND ADDRESS1 IS NULL");
                        if(rowdata.length > 4 && rowdata[4]!=null  && !rowdata[4].equals(""))paramSQL.append(" AND ADDRESS2 = '"+rowdata[4]+"'");else paramSQL.append(" AND ADDRESS2 IS NULL");
                        if(rowdata.length > 5 && rowdata[5]!=null  && !rowdata[5].equals(""))paramSQL.append(" AND ADDRESS3= '"+rowdata[5]+"'");else paramSQL.append(" AND ADDRESS3 IS NULL");
                        if(rowdata.length > 6 && rowdata[6]!=null  && !rowdata[6].equals(""))paramSQL.append(" AND CITY= '"+rowdata[6]+"'");else paramSQL.append(" AND CITY IS NULL");
                        if(rowdata.length > 7 && rowdata[7]!=null  && !rowdata[7].equals(""))paramSQL.append(" AND STATE = '"+rowdata[7]+"'");else paramSQL.append(" AND STATE IS NULL");    
                        if(rowdata.length > 8 && rowdata[8]!=null  && !rowdata[8].equals(""))paramSQL.append(" AND COUNTRY = '"+rowdata[8]+"'");else paramSQL.append(" AND COUNTRY IS NULL");
                        if(rowdata.length > 9 && rowdata[9]!=null  && !rowdata[9].equals(""))paramSQL.append(" AND POSTCODE = '"+rowdata[9]+"'");else paramSQL.append(" AND POSTCODE IS NULL");
                        if(rowdata.length > 10 && rowdata[10]!=null  && !rowdata[10].equals(""))paramSQL.append(" AND PHONE = '"+rowdata[10]+"'");else paramSQL.append(" AND PHONE IS NULL");
                        if(rowdata.length > 11 && rowdata[11]!=null  && !rowdata[11].equals(""))paramSQL.append(" AND FAX = '"+rowdata[11]+"'");else paramSQL.append(" AND FAX IS NULL");
                        if(rowdata.length > 12 && rowdata[12]!=null  && !rowdata[12].equals(""))paramSQL.append(" AND ACTIVE= '"+rowdata[12]+"'");else paramSQL.append(" AND ACTIVE IS NULL");
                        if(rowdata.length > 13 && rowdata[13]!=null  && !rowdata[13].equals(""))paramSQL.append(" AND LATITUDE= '"+rowdata[13]+"'");else paramSQL.append(" AND LATITUDE IS NULL");
                        if(rowdata.length > 14 && rowdata[14]!=null  && !rowdata[14].equals(""))paramSQL.append(" AND LONGITUDE = '"+rowdata[14]+"'");else paramSQL.append(" AND LONGITUDE IS NULL");
                        if(rowdata.length > 15 && rowdata[15]!=null  && !rowdata[15].equals(""))paramSQL.append(" AND IMAGE1= '"+rowdata[15]+"'");else paramSQL.append(" AND IMAGE1 IS NULL");
                        if(rowdata.length > 16 && rowdata[16]!=null  && !rowdata[16].equals(""))paramSQL.append(" AND INPUT_FILENAME= '"+rowdata[16]+"'");else paramSQL.append(" AND INPUT_FILENAME IS NULL");
                        paramSQL.append(")");
                        pstmt = getJdbcConnection().prepareStatement(UPDATE_STOREDATA_COMMENT_SQL+paramSQL.toString());         
                        break;
                    case XINT_MASDATA:                       
                        paramSQL.append("(SELECT XINT_MASDATA_ID FROM XINT_MASDATA WHERE  1=1 ");
                        if(rowdata.length > 0 && rowdata[0]!=null  && !rowdata[0].equals(""))paramSQL.append(" AND CPCODE = '"+rowdata[0]+"'");else paramSQL.append(" AND CPCODE IS NULL");    
                        if(rowdata.length > 1 && rowdata[1]!=null  && !rowdata[1].equals(""))paramSQL.append(" AND CPSOURCESKU = '"+rowdata[1]+"'");else paramSQL.append(" AND CPSOURCESKU IS NULL");                     
                        if(rowdata.length > 2 && rowdata[2]!=null  && !rowdata[2].equals(""))paramSQL.append(" AND MATYPE = '"+rowdata[2]+"'");else paramSQL.append(" AND MATYPE IS NULL");
                        if(rowdata.length > 3 && rowdata[3]!=null  && !rowdata[3].equals(""))paramSQL.append(" AND CPTARGETSKU = '"+rowdata[3]+"'");else paramSQL.append(" AND CPTARGETSKU IS NULL");
                        if(rowdata.length > 4 && rowdata[4]!=null  && !rowdata[4].equals(""))paramSQL.append(" AND INPUT_FILENAME = '"+rowdata[4]+"'");else paramSQL.append(" AND INPUT_FILENAME IS NULL");                      
                        paramSQL.append(")");
                        pstmt = getJdbcConnection().prepareStatement(UPDATE_MASDATA_COMMENT_SQL+paramSQL.toString());         
                        break;  
                    case XINT_OFFERSDATA:                                            
                        paramSQL.append("(SELECT XINT_OFFERSDATA_ID FROM XINT_OFFERSDATA WHERE  1=1 ");
                        if(rowdata.length > 0 && rowdata[0]!=null    && !rowdata[0].equals(""))paramSQL.append("  AND CPCODE = '"+rowdata[0]+"'");else paramSQL.append(" AND CPCODE IS NULL");    
                        if(rowdata.length > 1 && rowdata[1]!=null    && !rowdata[1].equals(""))paramSQL.append("  AND PRODUCTTYPE = '"+rowdata[1]+"'");else paramSQL.append(" AND PRODUCTTYPE IS NULL");
                        if(rowdata.length > 2 && rowdata[2]!=null    && !rowdata[2].equals(""))paramSQL.append("  AND CPSKU = '"+rowdata[2]+"'");else paramSQL.append(" AND CPSKU IS NULL");
                        if(rowdata.length > 3 && rowdata[3]!=null    && !rowdata[3].equals(""))paramSQL.append("  AND CPPARENTSKU = '"+rowdata[3]+"'");else paramSQL.append(" AND CPPARENTSKU IS NULL");
                        if(rowdata.length > 5 && rowdata[5]!=null    && !rowdata[5].equals(""))paramSQL.append("  AND NAME = '"+rowdata[5]+"'");else paramSQL.append(" AND NAME IS NULL");
                        if(rowdata.length > 6 && rowdata[6]!=null    && !rowdata[6].equals(""))paramSQL.append("  AND SHORTDESCRIPTION= '"+rowdata[6]+"'");else paramSQL.append(" AND SHORTDESCRIPTION IS NULL");
                        if(rowdata.length > 7 && rowdata[7]!=null    && !rowdata[7].equals(""))paramSQL.append("  AND LONGDESCRIPTION= '"+rowdata[7]+"'");else paramSQL.append(" AND LONGDESCRIPTION IS NULL");
                        if(rowdata.length > 8 && rowdata[8]!=null    && !rowdata[8].equals(""))paramSQL.append("  AND LISTPRICE = '"+rowdata[8]+"'");else paramSQL.append(" AND LISTPRICE IS NULL");    
                        if(rowdata.length > 9 && rowdata[9]!=null    && !rowdata[9].equals(""))paramSQL.append("  AND OFFERPRICE = '"+rowdata[9]+"'");else paramSQL.append(" AND OFFERPRICE IS NULL");
                        if(rowdata.length > 10 && rowdata[10]!=null  && !rowdata[10].equals(""))paramSQL.append(" AND CURRENCY = '"+rowdata[10]+"'");else paramSQL.append(" AND CURRENCY IS NULL");
                        if(rowdata.length > 11 && rowdata[11]!=null  && !rowdata[11].equals(""))paramSQL.append(" AND ONSPECIAL = '"+rowdata[11]+"'");else paramSQL.append(" AND ONSPECIAL IS NULL");
                        if(rowdata.length > 12 && rowdata[12]!=null  && !rowdata[12].equals(""))paramSQL.append(" AND PHYSICALSTOCK = '"+rowdata[12]+"'");else paramSQL.append(" AND PHYSICALSTOCK IS NULL");                        
                        if(rowdata.length > 13 && rowdata[13]!=null  && !rowdata[13].equals(""))paramSQL.append(" AND INVENTORY= '"+rowdata[13]+"'");else paramSQL.append(" AND INVENTORY IS NULL");
                        if(rowdata.length > 14 && rowdata[14]!=null  && !rowdata[14].equals(""))paramSQL.append(" AND IMAGE1= '"+rowdata[14]+"'");else paramSQL.append(" AND IMAGE1 IS NULL");
                        if(rowdata.length > 15 && rowdata[15]!=null  && !rowdata[15].equals(""))paramSQL.append(" AND IMAGE2= '"+rowdata[15]+"'");else paramSQL.append(" AND IMAGE2 IS NULL");
                        if(rowdata.length > 16 && rowdata[16]!=null  && !rowdata[16].equals(""))paramSQL.append(" AND IMAGE3 = '"+rowdata[16]+"'");else paramSQL.append(" AND IMAGE3 IS NULL");    
                        if(rowdata.length > 17 && rowdata[17]!=null  && !rowdata[17].equals(""))paramSQL.append(" AND IMAGE4 = '"+rowdata[17]+"'");else paramSQL.append(" AND IMAGE4 IS NULL");
                        if(rowdata.length > 18 && rowdata[18]!=null  && !rowdata[18].equals(""))paramSQL.append(" AND STARTDATE = '"+rowdata[18]+"'");else paramSQL.append(" AND STARTDATE IS NULL");
                        if(rowdata.length > 19 && rowdata[19]!=null  && !rowdata[19].equals(""))paramSQL.append(" AND ENDDATE = '"+rowdata[19]+"'");else paramSQL.append(" AND ENDDATE IS NULL");
                        if(rowdata.length > 20 && rowdata[20]!=null  && !rowdata[20].equals(""))paramSQL.append(" AND AGERESTRICTION = '"+rowdata[20]+"'");else paramSQL.append(" AND AGERESTRICTION IS NULL");
                        if(rowdata.length > 21 && rowdata[21]!=null  && !rowdata[21].equals(""))paramSQL.append(" AND BRAND= '"+rowdata[21]+"'");else paramSQL.append(" AND BRAND IS NULL");
                        if(rowdata.length > 22 && rowdata[22]!=null  && !rowdata[22].equals(""))paramSQL.append(" AND TARGETEDLOCATION= '"+rowdata[22]+"'");else paramSQL.append(" AND TARGETEDLOCATION IS NULL");
                        if(rowdata.length > 23 && rowdata[23]!=null  && !rowdata[23].equals(""))paramSQL.append(" AND TARGETEDPROXIMITY = '"+rowdata[23]+"'");else paramSQL.append(" AND TARGETEDPROXIMITY IS NULL");
                        if(rowdata.length > 24 && rowdata[24]!=null  && !rowdata[24].equals(""))paramSQL.append(" AND TARGETEDPOPULATION= '"+rowdata[24]+"'");else paramSQL.append(" AND TARGETEDPOPULATION IS NULL");
                        if(rowdata.length > 25 && rowdata[25]!=null  && !rowdata[25].equals(""))paramSQL.append(" AND KEYWORD= '"+rowdata[25]+"'");else paramSQL.append(" AND KEYWORD IS NULL");
                        if(rowdata.length > 26 && rowdata[26]!=null  && !rowdata[26].equals(""))paramSQL.append(" AND SEQUENCE= '"+rowdata[26]+"'");else paramSQL.append(" AND SEQUENCE IS NULL");
                        if(rowdata.length > 27 && rowdata[27]!=null  && !rowdata[27].equals(""))paramSQL.append(" AND INPUT_FILENAME= '"+rowdata[27]+"'");else paramSQL.append(" AND INPUT_FILENAME IS NULL");
                        paramSQL.append(")");
                        pstmt = getJdbcConnection().prepareStatement(UPDATE_OFFERSDATA_COMMENT_SQL+paramSQL.toString());         
                        break;                  
                    case XINT_LANGDATA:
                        paramSQL.append("(SELECT XINT_LANGDATA_ID FROM XINT_ADDLANGDATA WHERE  1=1 ");
                        if(rowdata.length > 0 && rowdata[0]!=null &&  !rowdata[0].equals(""))paramSQL.append(" AND CPCODE = '"+rowdata[0]+"'");else paramSQL.append(" AND CPCODE IS NULL");    
                        if(rowdata.length > 1 && rowdata[1]!=null &&  !rowdata[1].equals(""))paramSQL.append(" AND CPSKU = '"+rowdata[1]+"'");else paramSQL.append(" AND CPSKU IS NULL");
                        if(rowdata.length > 2 && rowdata[2]!=null  && !rowdata[2].equals(""))paramSQL.append(" AND LANGUAGE = '"+rowdata[2]+"'");else paramSQL.append(" AND LANGUAGE IS NULL");
                        if(rowdata.length > 3 && rowdata[3]!=null  && !rowdata[3].equals(""))paramSQL.append(" AND NAME = '"+rowdata[3]+"'");else paramSQL.append(" AND NAME IS NULL");                        
                        if(rowdata.length > 4 && rowdata[4]!=null  && !rowdata[4].equals(""))paramSQL.append(" AND SHORTDESCRIPTION= '"+rowdata[4]+"'");else paramSQL.append(" AND SHORTDESCRIPTION IS NULL");
                        if(rowdata.length > 5 && rowdata[5]!=null  && !rowdata[5].equals(""))paramSQL.append(" AND LONGDESCRIPTION= '"+rowdata[5]+"'");else paramSQL.append(" AND LONGDESCRIPTION IS NULL");
                        if(rowdata.length > 6 && rowdata[6]!=null  && !rowdata[6].equals(""))paramSQL.append(" AND INPUT_FILENAME = '"+rowdata[6]+"'");else paramSQL.append(" AND INPUT_FILENAME IS NULL");
                        paramSQL.append(")");
                        pstmt = getJdbcConnection().prepareStatement(UPDATE_ADDLANGDATA_COMMENT_SQL+paramSQL.toString());         
                        break;
                    case XINT_ATTRIBUTEDATA:
                        paramSQL.append("(SELECT XINT_ATTRDATA_ID FROM XINT_ATTRIBUTEDATA WHERE  1=1 ");
                        if(rowdata.length > 0 && rowdata[0]!=null && !rowdata[0].equals(""))paramSQL.append(" AND CPCODE = '"+rowdata[0]+"'");else paramSQL.append(" AND CPCODE IS NULL");    
                        if(rowdata.length > 1 && rowdata[1]!=null && !rowdata[1].equals(""))paramSQL.append(" AND PRODUCTTYPE = '"+rowdata[1]+"'");else paramSQL.append(" AND PRODUCTTYPE IS NULL");
                        if(rowdata.length > 2 && rowdata[2]!=null && !rowdata[2].equals(""))paramSQL.append(" AND CPSKU = '"+rowdata[2]+"'");else paramSQL.append(" AND CPSKU IS NULL");
                        if(rowdata.length > 3 && rowdata[3]!=null && !rowdata[3].equals(""))paramSQL.append(" AND ATTRIBUTENAME = '"+rowdata[3]+"'");else paramSQL.append(" AND ATTRIBUTENAME IS NULL");                        
                        if(rowdata.length > 4 && rowdata[4]!=null && !rowdata[4].equals(""))paramSQL.append(" AND ATTRIBUTEVALUE= '"+rowdata[4]+"'");else paramSQL.append(" AND ATTRIBUTEVALUE IS NULL");
                        if(rowdata.length > 5 && rowdata[5]!=null && !rowdata[5].equals(""))paramSQL.append(" AND SEQUENCE= '"+rowdata[5]+"'");else paramSQL.append(" AND SEQUENCE IS NULL");
                        if(rowdata.length > 6 && rowdata[6]!=null && !rowdata[6].equals(""))paramSQL.append(" AND INPUT_FILENAME = '"+rowdata[6]+"'");else paramSQL.append(" AND INPUT_FILENAME IS NULL");
                        paramSQL.append(")");
                        pstmt = getJdbcConnection().prepareStatement(UPDATE_ATTRIBUTEDATA_COMMENT_SQL+paramSQL.toString());         
                        break;
                    case XINT_PROMOTIONDATA:
                        paramSQL.append("(SELECT XINT_PROMODATA_ID FROM XINT_PROMOTIONDATA WHERE  1=1 ");
                        if(rowdata.length > 0 && rowdata[0]!=null && !rowdata[0].equals(""))paramSQL.append(" AND CPCODE = '"+rowdata[0]+"'");else paramSQL.append(" AND CPCODE IS NULL");    
                        if(rowdata.length > 1 && rowdata[1]!=null && !rowdata[1].equals(""))paramSQL.append(" AND CPSKU = '"+rowdata[1]+"'");else paramSQL.append(" AND CPSKU IS NULL");
                        if(rowdata.length > 2 && rowdata[2]!=null && !rowdata[2].equals(""))paramSQL.append(" AND PRODUCTTYPE = '"+rowdata[2]+"'");else paramSQL.append(" AND PRODUCTTYPE IS NULL");
                        if(rowdata.length > 3 && rowdata[3]!=null && !rowdata[3].equals(""))paramSQL.append(" AND PERCENTAGEOFF = '"+rowdata[3]+"'");else paramSQL.append(" AND PERCENTAGEOFF IS NULL");                        
                        if(rowdata.length > 4 && rowdata[4]!=null && !rowdata[4].equals(""))paramSQL.append(" AND AMOUNTOFF= '"+rowdata[4]+"'");else paramSQL.append(" AND AMOUNTOFF IS NULL");
                        if(rowdata.length > 5 && rowdata[5]!=null && !rowdata[5].equals(""))paramSQL.append(" AND SHIPQUALAMOUNT= '"+rowdata[5]+"'");else paramSQL.append(" AND SHIPQUALAMOUNT IS NULL");
                        if(rowdata.length > 6 && rowdata[6]!=null && !rowdata[6].equals(""))paramSQL.append(" AND FROMDATE = '"+rowdata[6]+"'");else paramSQL.append(" AND FROMDATE IS NULL");
                        if(rowdata.length > 7 && rowdata[7]!=null && !rowdata[7].equals(""))paramSQL.append(" AND TODATE = '"+rowdata[7]+"'");else paramSQL.append(" AND TODATE IS NULL");
                        if(rowdata.length > 8 && rowdata[8]!=null && !rowdata[8].equals(""))paramSQL.append(" AND PROMOTION_NAME = '"+rowdata[8]+"'");else paramSQL.append(" AND PROMOTION_NAME IS NULL");
                        if(rowdata.length > 9 && rowdata[9]!=null && !rowdata[9].equals(""))paramSQL.append(" AND INPUT_FILENAME = '"+rowdata[9]+"'");else paramSQL.append(" AND INPUT_FILENAME IS NULL");
                        paramSQL.append(")");
                        pstmt = getJdbcConnection().prepareStatement(UPDATE_PROMOTIONDATA_COMMENT_SQL+paramSQL.toString());         
                        break;
                
                }
                pstmt.setString(PARAM_1, comment);
                pstmt.setString(PARAM_2, status);
                LOGGER.logp(Level.INFO, CLASSNAME, "updateErrorCode", "updating comments::"+comment+" where"+paramSQL.toString());               
               
                int record=pstmt.executeUpdate();
                LOGGER.logp(Level.INFO, CLASSNAME, "updateErrorCode", " Updated record count "+record + "  Ex: 1 Success 0 Failure" );
                
        } catch (SQLException sqle) {           
            throw sqle;           
        } finally {        
            if(pstmt!=null){pstmt.close();pstmt=null;}
        }
        
        LOGGER.exiting(CLASSNAME, "updateErrorCode");
    }
   
    
    /*              
     * This method update all xint comment column for error code 
     */
    
    public void updateErrorCodeBySeq(TableName tableName, String name, String  seqNo, String comment, String status) throws SQLException{
        String methodName = "updateErrorCode";
        LOGGER.entering(CLASSNAME, methodName); 
        LOGGER.logp(Level.INFO, CLASSNAME, methodName, "tableName:" + tableName + " name:" + name + " seqNo:" + seqNo + " comment:" + comment + " status:" + status);               
        PreparedStatement pstmt = null;

        try{
                switch(tableName){
                    case XINT_STOCKDATA:
                        // Code needs to go here if we implement sequence no
                        break;
                    case XINT_PRICEDATA:
                        // Code needs to go here if we implement sequence no
                        break;
                    case XINT_STORESDATA:                        
                        // Code needs to go here if we implement sequence no
                        break;
                    case XINT_MASDATA:                       
                        // Code needs to go here if we implement sequence no
                        break;  
                    case XINT_OFFERSDATA:                                            
                        pstmt = getJdbcConnection().prepareStatement(UPDATE_OFFERSDATA_COMMENT_SQL);         
                        break;                  
                    case XINT_LANGDATA:
                        // Code needs to go here if we implement sequence no
                        break;
                    case XINT_ATTRIBUTEDATA:
                        pstmt = getJdbcConnection().prepareStatement(UPDATE_ATTRIBUTEDATA_COMMENT_SQL);  
                        break;
                    case XINT_PROMOTIONDATA:
                        // Code needs to go here if we implement sequence no
                        break;
                
                }
                pstmt.setString(PARAM_1, comment);
                pstmt.setString(PARAM_2, status);
                pstmt.setString(PARAM_3, seqNo);
                pstmt.setString(PARAM_4, name);
               
                int record=pstmt.executeUpdate();
                LOGGER.logp(Level.INFO, CLASSNAME, methodName, "record updated:="+record);
        } catch (SQLException sqle) {           
            throw sqle;           
        } finally {        
            if(pstmt!=null){pstmt.close();pstmt=null;}
        }
        
        LOGGER.exiting(CLASSNAME, methodName);
    }
    
    
    
    /* 
     * This method update all xint comment column for error code 
     */
    
    public List<String> getErrorCodeWithCSVData(TableName tableName, String fileName) throws SQLException{
        LOGGER.entering(CLASSNAME, "getErrorCodeWithCSVData", tableName);
         List<String> listData=new ArrayList<String>();
         ResultSetMetaData metadata=null; 
         PreparedStatement pstmt = null;
         ResultSet res=null;
         StringBuffer sbCSVData=null;        
        try{
                switch(tableName){
                    case XINT_STOCKDATA:                
                        pstmt = getJdbcConnection().prepareStatement(LOG_STOCKDATA_SQL);               
                        break;
                    case XINT_PRICEDATA:
                        pstmt = getJdbcConnection().prepareStatement(LOG_PRICEDATA_SQL);             
                        break;
                    case XINT_STORESDATA:
                        pstmt = getJdbcConnection().prepareStatement(LOG_STORESDATA_SQL);               
                        break;
                    case XINT_MASDATA:
                        pstmt = getJdbcConnection().prepareStatement(LOG_MASDATA_SQL);                
                        break;    
                    case XINT_OFFERSDATA:
                        pstmt = getJdbcConnection().prepareStatement(LOG_OFFERSDATA_SQL);                
                        break;
                    case XINT_LANGDATA:
                        pstmt = getJdbcConnection().prepareStatement(LOG_LANGDATA_SQL);                
                        break;
                    case XINT_ATTRIBUTEDATA:
                        pstmt = getJdbcConnection().prepareStatement(LOG_ATTRIBUTEDATA_SQL);                
                        break;
                    case XINT_PROMOTIONDATA:
                        pstmt = getJdbcConnection().prepareStatement(LOG_PROMOTIONDATA_SQL);                
                        break;   
                    case XINT_OFFERSATTRIBUTEDATA:
                        pstmt = getJdbcConnection().prepareStatement(LOG_OFFER_ATTRIBUTE_DATA_SQL);                
                        break;     
                        
                }
                pstmt.setString(PARAM_1, fileName);
                res=pstmt.executeQuery();  
                metadata=res.getMetaData();
                int colCount=metadata.getColumnCount();
                sbCSVData=new StringBuffer();
                
                for(int h=1; h<=colCount; h++){
                    sbCSVData.append(metadata.getColumnName(h)!=null?metadata.getColumnName(h):"");
                    sbCSVData.append("|");
                }   
                
                listData.add(sbCSVData.toString());    
                while(res.next()){                    
                    sbCSVData=new StringBuffer();
                    for(int r=1; r<=colCount; r++){
                        sbCSVData.append(res.getString(r)!=null?res.getString(r):"");
                        sbCSVData.append("|");
                    }
                    
                    listData.add(sbCSVData.toString());
                }   
        } catch (SQLException sqle) {           
            throw sqle;           
        } finally {
            if(res!=null){res.close();res=null;}
            if(pstmt!=null){pstmt.close();pstmt=null;}
        }
        LOGGER.exiting(CLASSNAME, "getErrorCodeWithCSVData");
        return listData;        
    }
    
    
    
    /**
     * This method created a parent product record in the xint_offersdata table if the CPPARENTSKU does not exist in commerce.
     * @throws SQLException Exception
     */
    public void createParentProduct() throws SQLException{
        String methodName = "createParentProduct";
        LOGGER.entering(CLASSNAME, methodName);
        PreparedStatement pstmt  = null;
        PreparedStatement pstmt1 = null;
        ResultSet         rs     = null;
        
        try{      
            pstmt = getJdbcConnection().prepareStatement(FIND_NON_EXISTENT_PRODUCTS_SQL);
            rs= pstmt.executeQuery();
            
            while(rs.next()){
                String cpSku  = rs.getString(PARAM_1);
                String cpCode = rs.getString(PARAM_2);
                LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Create Parent product, sku:" + cpSku + " cpCode:" + cpCode);
                if(cpSku!=null){
                    pstmt1 = getJdbcConnection().prepareStatement(CREATE_PARENT_PRODUCTS_SQL);
                    pstmt1.setString(PARAM_1, cpSku);
                    pstmt1.setString(PARAM_2, cpSku);
                    pstmt1.setString(PARAM_3, cpCode);
                    int recordInserted=pstmt1.executeUpdate();
                    LOGGER.logp(Level.INFO, CLASSNAME, methodName, " inserted new parent product into xint_offersdata "+recordInserted );
                }
            }     
        }catch (SQLException sqle) {           
            throw sqle;           
        } finally {
            if(rs!=null){rs.close();rs=null;}
            if(pstmt!=null){pstmt.close();pstmt=null;}
            if(pstmt1!=null){pstmt1.close();pstmt1=null;}
        }        
    }
    
    
    
    
    
    
    
    
    
    
    

    /**
     * This method created parent product attribute records in the xint_attributedata table if the CPPARENTSKU does not exist in commerce.
     * @throws SQLException Exception
     */
    public void createParentProductAttribute() throws SQLException{
        String methodName = "createParentProductAttribute";
        LOGGER.entering(CLASSNAME, methodName);
        
        PreparedStatement pstmt  = null;
        PreparedStatement pstmt1 = null;
        PreparedStatement pstmt2 = null;
        ResultSet         rs     = null;
        ResultSet         rs1    = null;
        
        try {      
            pstmt = getJdbcConnection().prepareStatement(FIND_PRODUCTS_WITHOUT_ATTRIBUTES_SQL);
            rs    = pstmt.executeQuery();
            
            while(rs.next()){
                String cpParentSku = rs.getString(PARAM_1);
                String cpCode      = rs.getString(PARAM_2); 
                LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Create parent product attributes sku:" + cpParentSku + " cpCode:" + cpCode);
                
                if (cpParentSku != null){
                    pstmt1 = getJdbcConnection().prepareStatement(FIND_ITEM_ATTRIBUTE_SQL);
                    pstmt1.setString(PARAM_1, cpParentSku);
                    pstmt1.setString(PARAM_2, cpParentSku);
                    pstmt1.setString(PARAM_3, cpCode);
                    rs1 = pstmt1.executeQuery();
                    
                    while(rs1.next()) {
                        pstmt2=getJdbcConnection().prepareStatement(INSERT_PRODUCT_ATTRIBUTE_SQL);
                        pstmt2.setString(PARAM_1, rs1.getString(PARAM_1));
                        pstmt2.setString(PARAM_2, rs1.getString(PARAM_2));
                        pstmt2.setString(PARAM_3, rs1.getString(PARAM_3));
                        pstmt2.setString(PARAM_4, rs1.getString(PARAM_4));
                        pstmt2.setString(PARAM_5, rs1.getString(PARAM_5));
                        pstmt2.setString(PARAM_6, rs1.getString(PARAM_6));
                        pstmt2.setString(PARAM_7, rs1.getString(PARAM_7));
                        pstmt2.setString(PARAM_8, rs1.getString(PARAM_8));
                        int recordInserted=pstmt2.executeUpdate();
                        LOGGER.logp(Level.INFO, CLASSNAME, methodName, " inserted new parent attribute record into xint_attributedata "+recordInserted );
                    }
                }
            }     
        } catch (SQLException sqle) {           
            throw sqle;           
        } finally {
            if(rs!=null){rs.close();rs=null;}
            if(rs1!=null){rs1.close();rs1=null;}
            if(pstmt!=null){pstmt.close();pstmt=null;}
            if(pstmt1!=null){pstmt1.close();pstmt1=null;}
            if(pstmt2!=null){pstmt2.close();pstmt2=null;}
        }        
    }
    
    /**
     * This method retrieves the catalog identifier for a given store identifier.
     * @throws SQLException Exception
     */
    public String getCatalogIdentifier(String cpCode) throws SQLException{
        LOGGER.entering(CLASSNAME, "getStoreAndCatalogIdentifier");
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String catalogIdentifier=null;
        try{      
            pstmt = getJdbcConnection().prepareStatement(FIND_CATALOG_IDENTIFIER_SQL);
            pstmt.setString(PARAM_1, cpCode);
            rs= pstmt.executeQuery();
            while(rs.next()){
                catalogIdentifier=rs.getString(PARAM_1);
                LOGGER.logp(Level.INFO, CLASSNAME, "getCatalogIdentifier", "Catalog Identifier= "+catalogIdentifier );
                
            }     
        }catch (SQLException sqle) {           
            throw sqle;           
        } finally {
            if(rs!=null){rs.close();rs=null;}
            if(pstmt!=null){pstmt.close();pstmt=null;}
        }
        return catalogIdentifier;
    }
    
    /**
     * This method retrieves the allowed Attributes for a particular product type.
     * @throws SQLException Exception
     */
    
    public String getAllowedAttributes(final String productType) throws SQLException{
        String methodName = "getAllowedAttributes";
        LOGGER.entering(CLASSNAME, methodName);
        
        PreparedStatement pstmt = null;
        ResultSet         rs    = null;
        
        String attributeValue = null;
        String productTypeFields = productType + "Fields";
        try{      
            pstmt = getJdbcConnection().prepareStatement(FIND_ATTRIBUTE_VALUE_SQL);
            pstmt.setString(PARAM_1, productTypeFields);
            rs = pstmt.executeQuery();
            while(rs.next()){
                attributeValue = rs.getString(PARAM_1);
                LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Attribute value:" + attributeValue );                
            }     
        }catch (SQLException sqle) {           
            throw sqle;           
        } finally {
            if(rs!=null){rs.close();rs=null;}
            if(pstmt!=null){pstmt.close();pstmt=null;}
        }
        return attributeValue;
    }
    
    /**
     * This method retrieves the attribute value for a particular attribute.
     * @throws SQLException Exception
     */
    
    public String getCategorySelection(final String categoryName, final String sequenceNo) throws SQLException{
        String methodName = "getCategorySelection";
        LOGGER.entering(CLASSNAME, methodName);
        
        PreparedStatement pstmt = null;
        ResultSet         rs    = null;
        
        String attributeValue = null;
        try{      
            pstmt = getJdbcConnection().prepareStatement(FIND_XINT_CATEGORY_SQL);
            pstmt.setString(PARAM_1, categoryName);
            pstmt.setString(PARAM_2, sequenceNo);
            rs = pstmt.executeQuery();
            while(rs.next()){
                attributeValue = rs.getString(PARAM_1);
                LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Attribute value:" + attributeValue );                
            }     
        }catch (SQLException sqle) {           
            throw sqle;           
        } finally {
            if(rs!=null){rs.close();rs=null;}
            if(pstmt!=null){pstmt.close();pstmt=null;}
        }
        return attributeValue;
    }   
    
    /**
     * This method retrieves the validation status of the offer record.
     * @throws SQLException Exception
     */
    
    public String getOfferStatus(final String sequenceNo) throws SQLException{
        String methodName = "getOfferStatus";
        LOGGER.entering(CLASSNAME, methodName);
        
        PreparedStatement pstmt = null;
        ResultSet         rs    = null;
        
        String status = null;
        try{      
            pstmt = getJdbcConnection().prepareStatement(FIND_OFFER_STATUS_SQL);
            pstmt.setString(PARAM_1, sequenceNo);
            rs = pstmt.executeQuery();
            while(rs.next()){
                status = rs.getString(PARAM_1);
                LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Sequence:" + sequenceNo + " Status:" + status );                
            }     
        }catch (SQLException sqle) {           
            throw sqle;           
        } finally {
            if(rs!=null){rs.close();rs=null;}
            if(pstmt!=null){pstmt.close();pstmt=null;}
        }
        return status;
    }     
    
    /**
     * This method updates the processed flag to 2 on the xint_offersdata record.
     * @throws SQLException Exception
     */
    public void setOfferToFailed(final String sequenceNo) throws SQLException{
        String methodName = "setOfferToFailed";
        LOGGER.entering(CLASSNAME, methodName);
        
        PreparedStatement pstmt = null;
        ResultSet         rs    = null;
        
        String fileName  = null;
        String parentSku = null;
        
        try {
            pstmt = getJdbcConnection().prepareStatement(UPDATE_OFFERS_PROCESSED_FLAG_FAILED_SQL);
            pstmt.setString(PARAM_1, sequenceNo);            
            pstmt.executeUpdate(); 
            
            pstmt = getJdbcConnection().prepareStatement(FIND_PARENT_PRODUCT_ATTRS_SQL);
            pstmt.setString(PARAM_1, sequenceNo);
            rs = pstmt.executeQuery();
            while(rs.next()){
                fileName  = rs.getString(PARAM_1);
                parentSku = rs.getString(PARAM_2);
                LOGGER.logp(Level.INFO, CLASSNAME, methodName, "fileName:" + fileName + "  parentSku:" + parentSku ); 
            }     
            
            if (parentSku != null && parentSku.trim().length() > 0) {
                
                pstmt = getJdbcConnection().prepareStatement(UPDATE_PARENT_OFFERS_PROCESSED_FLAG_FAILED_SQL);
                pstmt.setString(PARAM_1, fileName);            
                pstmt.setString(PARAM_2, parentSku);
                pstmt.setString(PARAM_3, parentSku);
                pstmt.executeUpdate();                
            }            
            
        }catch (SQLException sqle) {           
            throw sqle;           
        } finally {
            if(pstmt!=null){pstmt.close();pstmt=null;}
            if(rs!=null){rs.close();rs=null;}
        }
        
        LOGGER.exiting(CLASSNAME, methodName);
        
    }    
    
    /**
     * This method updates the processed flag to 2 on the xint_offersdata record.
     * @throws SQLException Exception
     */
    public void setAllAttributesToFailed(final String sequenceNo) throws SQLException{
        String methodName = "setAllAttributesToFailed";
        LOGGER.entering(CLASSNAME, methodName);
        
        PreparedStatement pstmt = null;
        
        try {
            
            LOGGER.logp(Level.INFO, CLASSNAME, methodName, " setAllAttributesToFailed sequenceNo:" + sequenceNo );
            
            pstmt = getJdbcConnection().prepareStatement(UPDATE_ATTRIBUTES_PROCESSED_FLAG_FAILED_SQL);
            pstmt.setString(PARAM_1, sequenceNo);
            
            int record = pstmt.executeUpdate(); 
            
        } catch (SQLException sqle) {           
            throw sqle;           
        } finally {
            if(pstmt!=null){pstmt.close();pstmt=null;}
        }
        
        LOGGER.exiting(CLASSNAME, methodName);
        
    }
    
    /**
     * This method retrieves the max sequence number from the xint_attributedata table.
     * @throws SQLException Exception
     */
    public int getSequence() throws SQLException{
        LOGGER.entering(CLASSNAME, "getSequence");
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        int sequence=0;
        try{      
            pstmt = getJdbcConnection().prepareStatement(FIND_MAX_SEQUENCE_SQL);
            rs= pstmt.executeQuery();
            while(rs.next()){
                sequence=rs.getInt(PARAM_1);
                LOGGER.logp(Level.INFO, CLASSNAME, "getSequence", "Maximum Sequence Number= "+sequence );
                
            }     
        }catch (SQLException sqle) {           
            throw sqle;           
        } finally {
            if(rs!=null){rs.close();rs=null;}
            if(pstmt!=null){pstmt.close();pstmt=null;}
        }
        return sequence;
    }
    
    /**
     * This method retrieves the list of all attributes for a given product from the xint_attributedata table.
     * @throws SQLException Exception
     */
    public String getAllAttributes(String sequenceNo) throws SQLException{
        LOGGER.entering(CLASSNAME, "getSequence");
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String attributeList="";
        try{      
            pstmt = getJdbcConnection().prepareStatement(FIND_ALL_PRODUCT_ATTRIBUTES_SQL);
            pstmt.setString(PARAM_1, sequenceNo);
            rs= pstmt.executeQuery();
            while(rs.next()){
                attributeList=attributeList+"|"+rs.getString(PARAM_1);
                LOGGER.logp(Level.INFO, CLASSNAME, "getAllAttributes", "Product Attribtue=" +attributeList);
                
            }     
        }catch (SQLException sqle) {           
            throw sqle;           
        } finally {
            if(rs!=null){rs.close();rs=null;}
            if(pstmt!=null){pstmt.close();pstmt=null;}
        }
        return attributeList;
    }
    /**
     * This method retrieves the list of all attributes for a given product from the xint_attributedata table.
     * @throws SQLException Exception
     */
    public String getProcessedFlag(String sequenceNo, String attributeName) throws SQLException{
        LOGGER.entering(CLASSNAME, "getSequence");
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String processed="";
        try{      
            pstmt = getJdbcConnection().prepareStatement(FIND_PROCESSED_FLAG_SQL);
            pstmt.setString(PARAM_1, sequenceNo);
            pstmt.setString(PARAM_2, attributeName);
            rs= pstmt.executeQuery();
            while(rs.next()){
                processed=rs.getString(PARAM_1);
                LOGGER.logp(Level.INFO, CLASSNAME, "getAllAttributes", "Product Attribtue=" +processed);
                
            }     
        }catch (SQLException sqle) {           
            throw sqle;           
        } finally {
            if(rs!=null){rs.close();rs=null;}
            if(pstmt!=null){pstmt.close();pstmt=null;}
        }
        return processed;
    }
    /**
     * close
     * @throws SQLException Exception
     */
    public void close() throws SQLException {
        closeConnection();
    }

    /**
     * commit
     * @throws SQLException Exception
     */
    public void commit() throws SQLException {
        commitTransaction();
    }
    /**
     * 
     * TableName return 
     */

    /**
     * This method updates the processed flag to 3 once data is loaded to commerce.
     * @throws SQLException Exception
     */
    public void updateProcessedFlag(TableName tableName, String [] dataArray) throws SQLException{
        
        String methodName = "updateProcessedFlag";
        LOGGER.entering(CLASSNAME, methodName);
        LOGGER.logp(Level.INFO, CLASSNAME, methodName, " tableName:" + tableName + " dataArray.length:" + dataArray.length);
        
        PreparedStatement pstmt = null;        
        switch(tableName){
            case XINT_STOCKDATA:  
                pstmt = getJdbcConnection().prepareStatement(UPDATE_STOCK_PROCESSED_FLAG_SQL);
                pstmt.setString(PARAM_1, dataArray[2]);
                pstmt.setString(PARAM_2, dataArray[4]);
                pstmt.setString(PARAM_3, dataArray[5]);
                break;
            case XINT_PRICEDATA:
                pstmt = getJdbcConnection().prepareStatement(UPDATE_PRICE_PROCESSED_FLAG_SQL);  
                pstmt.setString(PARAM_1, dataArray[1]);
                pstmt.setString(PARAM_2, dataArray[4]);
                pstmt.setString(PARAM_3, dataArray[5]);
                pstmt.setString(PARAM_4, dataArray[8]);
                break;
            case XINT_STORESDATA:
                pstmt = getJdbcConnection().prepareStatement(UPDATE_STORES_PROCESSED_FLAG_SQL);   
                pstmt.setString(PARAM_1, dataArray[0]);
                pstmt.setString(PARAM_2, dataArray[1]);
                pstmt.setString(PARAM_3, dataArray[2]);
                break;
            case XINT_MASDATA:
                pstmt = getJdbcConnection().prepareStatement(UPDATE_MAS_PROCESSED_FLAG_SQL); 
                pstmt.setString(PARAM_1, dataArray[1]);
                pstmt.setString(PARAM_2, dataArray[2]);
                pstmt.setString(PARAM_3, dataArray[3]);
                break;  
            case XINT_OFFERSDATA:
                pstmt = getJdbcConnection().prepareStatement(UPDATE_OFFERS_PROCESSED_FLAG_SQL);  
                pstmt.setString(PARAM_1, dataArray[4]);
                break;                  
            case XINT_LANGDATA:
                pstmt = getJdbcConnection().prepareStatement(UPDATE_ADDLANG_PROCESSED_FLAG_SQL);   
                pstmt.setString(PARAM_1, dataArray[1]);
                pstmt.setString(PARAM_2, dataArray[2]);
                pstmt.setString(PARAM_3, dataArray[3]);
                break;
            case XINT_ATTRIBUTEDATA:
                pstmt = getJdbcConnection().prepareStatement(UPDATE_ATTRIBUTE_PROCESSED_FLAG_SQL);
                pstmt.setString(PARAM_1, dataArray[6]);
                break;
            case XINT_PROMOTIONDATA:
                pstmt = getJdbcConnection().prepareStatement(UPDATE_PROMOTION_PROCESSED_FLAG_SQL);         
                break;        
        }

        try{      
            pstmt.executeUpdate();    
        }catch (SQLException sqle) {           
            throw sqle;           
        } finally {
            if(pstmt!=null){pstmt.close();pstmt=null;}
        }
        
        LOGGER.exiting(CLASSNAME, methodName);
        
    }
    
    
    
}

