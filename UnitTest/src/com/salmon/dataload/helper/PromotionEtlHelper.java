package com.salmon.dataload.helper;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PromotionEtlHelper extends BaseJDBCHelper {
    private static final String CLASSNAME = PromotionEtlHelper.class.getName();
    private static final Logger LOGGER = Logger.getLogger(CLASSNAME);
    
    public static final String UPDATE_DATA_XINTPROMOTION_SQL ="UPDATE XINT_PROMOTIONDATA SET PROCESSED=4 WHERE XINT_PROMODATA_ID=?";
    public static final String FIND_DATA_XINTPROMOTION_SQL ="SELECT * FROM XINT_PROMOTIONDATA WHERE PROCESSED = 0 FETCH FIRST 1 ROW ONLY";
    public static final String GET_CATENTRY_ID_SQL ="SELECT CATENTRY_ID FROM CATENTRY WHERE PARTNUMBER=?";
    public static final String MAX_PX_ELEMENT_ID_SQL ="select max(px_element_id) from px_element";

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
    
    
    public static final String FIND_NON_EXISTENT_PRODUCTS_SQL = "SELECT DISTINCT X1.CPPARENTSKU, X1.CPCODE " + 
    "  FROM XINT_OFFERSDATA X1 " +
    " WHERE X1.PROCESSED   = 1 " +
    "   AND X1.CPPARENTSKU IS NOT NULL " +
    "   AND NOT EXISTS (SELECT 1 " + 
    "                     FROM XINT_OFFERSDATA X2  " + 
    "                    WHERE X2.CPSKU     = X1.CPPARENTSKU " +
    "                      AND X2.PROCESSED = 1) WITH UR ";       

    
    
    public static final String MAX_PX_ELEMENTNVP_ID_SQL ="select max(px_elementnvp_id) from px_elementnvp";
    
    public static final String GET_DATA_PX_PROMOTION_SQL ="SELECT PX_PROMOTION_ID FROM PX_PROMOTION WHERE NAME=?";
    
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
    
    TableName name;
    
    /**
     * MonitiseEtlHelper
     * @param jdbcDriver String
     * @param jdbcUrl String
     * @param dbUser String
     * @param dbPassword String
     * @throws Exception Exception
     */
    
    public PromotionEtlHelper(final String jdbcDriver, final String jdbcUrl, 
            final String dbUser, final String dbPassword) throws ClassNotFoundException, SQLException {
           makeConnection(jdbcDriver, jdbcUrl, dbUser, dbPassword);
    }    
    
    public PromotionEtlHelper(TableName name)  {
        this.name=name;
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
     * This method created a promotion XML file from XINT_PROMOTIONDATA table
     * and will be used by the OOB utility to load the promotion.
     * 
     * @throws SQLException Exception
     */
    public String  getCatentryId(String cpSKU) throws SQLException {
        LOGGER.entering(CLASSNAME, "getCatentryId");
        PreparedStatement pstmt = null;
        
        String maxPxElemenId=null;
        ResultSet rs=null ;
        try {
            pstmt = getJdbcConnection().prepareStatement(GET_CATENTRY_ID_SQL);
            pstmt.setString(PARAM_1, cpSKU);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                maxPxElemenId = rs.getString(PARAM_1);
            }
            return maxPxElemenId;
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
     * This method created a promotion XML file from XINT_PROMOTIONDATA table
     * and will be used by the OOB utility to load the promotion.
     * 
     * @throws SQLException Exception
     */
    public String  maxPxElementId(String promoId) throws SQLException {
        LOGGER.entering(CLASSNAME, "maxPxElementId");
        PreparedStatement pstmt = null;
        PreparedStatement pstmt1 = null;
        String maxPxElemenId=null;
        ResultSet rs=null ;
        try {
            pstmt = getJdbcConnection().prepareStatement(MAX_PX_ELEMENT_ID_SQL);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                maxPxElemenId = rs.getString(PARAM_1);
            }
            return maxPxElemenId;
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
     * This method created a promotion XML file from XINT_PROMOTIONDATA table
     * and will be used by the OOB utility to load the promotion.
     * 
     * @throws SQLException Exception
     */
    public String  maxPxElementNVPId(String promoId) throws SQLException {
        LOGGER.entering(CLASSNAME, "maxPxElementNVPId");
        PreparedStatement pstmt = null;
        PreparedStatement pstmt1 = null;
        String maxPxElemenId=null;
        ResultSet rs=null ;
        try {
            pstmt = getJdbcConnection().prepareStatement(MAX_PX_ELEMENTNVP_ID_SQL);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                maxPxElemenId = rs.getString(PARAM_1);
            }
            return maxPxElemenId;
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
     * This method created a promotion XML file from XINT_PROMOTIONDATA table
     * and will be used by the OOB utility to load the promotion.
     * 
     * @throws SQLException Exception
     */
    public String  getPxPromotionId(String promotionName) throws SQLException {
        LOGGER.entering(CLASSNAME, "getPxPromotionId");
        PreparedStatement pstmt = null;
        PreparedStatement pstmt1 = null;
        String maxPxElemenId=null;
        ResultSet rs=null ;
        try {
            pstmt = getJdbcConnection().prepareStatement(GET_DATA_PX_PROMOTION_SQL);
            pstmt.setString(PARAM_1, promotionName);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                maxPxElemenId = rs.getString(PARAM_1);
            }
            return maxPxElemenId;
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
     * This method created a promotion XML file from XINT_PROMOTIONDATA table
     * and will be used by the OOB utility to load the promotion.
     * 
     * @throws SQLException Exception
     */
    public String[] createPromotionXML() throws SQLException {
        LOGGER.entering(CLASSNAME, "createPromotionXML");
        PreparedStatement pstmt  = null;
        PreparedStatement pstmt1 = null;
        ResultSet         rs     = null;
        
        String promotionXML   = null;
        String promotionId    = null;
        String cpCode         = null;
        String cpSKU          = null;
        String productType    = null;
        String percentageOff  = null;
        String amountOff      = null;
        String shipQualAmount = null;
        String fromDate       = null;
        String toDate         = null;
        String promotionName  = null;

        try {
            pstmt = getJdbcConnection().prepareStatement(FIND_DATA_XINTPROMOTION_SQL);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                promotionId    = rs.getString(PARAM_1);
                cpCode         = rs.getString(PARAM_2);
                cpSKU          = rs.getString(PARAM_3);
                productType    = rs.getString(PARAM_4);
                percentageOff  = rs.getString(PARAM_5);
                amountOff      = rs.getString(PARAM_6);
                shipQualAmount = rs.getString(PARAM_7);
                fromDate       = rs.getString(PARAM_8);
                toDate         = rs.getString(PARAM_9);
                promotionName  = rs.getString(PARAM_10);
            }
           // productType="P";
            
            if (productType.equals("P") && amountOff!=null){
            promotionXML = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Promotion impl=\"com.ibm.commerce.marketing.promotion.DefaultPromotion\"><PromotionKey><PromotionName>"+promotionName +"</PromotionName><StoreKey><DN>ou=asset store organization,o=extended sites organization,o=root organization</DN><Identifier>monitiseStorefrontAssetStore</Identifier></StoreKey><Version>0</Version><Revision>0</Revision></PromotionKey><PromotionGroupKey><GroupName>ProductLevelPromotion</GroupName><StoreKey><DN>ou=asset store organization,o=extended sites organization,o=root organization</DN><Identifier>monitiseStorefrontAssetStore</Identifier></StoreKey></PromotionGroupKey><TypedNLDescription impl=\"com.ibm.commerce.marketing.promotion.TypedNLDescription\"><DefaultLocale>en_US</DefaultLocale><Description locale=\"en_US\" type=\"admin\">"+promotionName+"</Description></TypedNLDescription><Priority>0</Priority><Exclusive>0</Exclusive><ExemptPolicyList/><ExplicitlyAppliedPolicyList/><Status>1</Status><LastUpdate>16-07-2014 09:13:08</LastUpdate><LastUpdateBy><CustomerKey><LogonId>wcsadmin</LogonId></CustomerKey></LastUpdateBy><PerOrderLimit>-1</PerOrderLimit><PerShopperLimit>-1</PerShopperLimit><ApplicationLimit>-1</ApplicationLimit><TargetSales>0</TargetSales><CorrespondingRBDTypeName>ProductLevelPerItemValueDiscount</CorrespondingRBDTypeName><Schedule impl=\"com.ibm.commerce.marketing.promotion.schedule.PromotionSchedule\"><DateRange impl=\"com.ibm.commerce.marketing.promotion.schedule.DateRangeSchedule\"><Start inclusive=\"true\">"+fromDate+"</Start><End inclusive=\"true\">"+toDate+"</End></DateRange><TimeWithinADay impl=\"com.ibm.commerce.marketing.promotion.schedule.TimeRangeWithinADaySchedule\"><Start inclusive=\"true\">00:00:00</Start><End inclusive=\"true\">23:59:59</End></TimeWithinADay><Week impl=\"com.ibm.commerce.marketing.promotion.schedule.WeekDaySchedule\"><WeekDay>SUNDAY</WeekDay><WeekDay>MONDAY</WeekDay><WeekDay>TUESDAY</WeekDay><WeekDay>WEDNESDAY</WeekDay><WeekDay>THURSDAY</WeekDay><WeekDay>FRIDAY</WeekDay><WeekDay>SATURDAY</WeekDay></Week></Schedule><PromotionType>0</PromotionType><PromotionCodeRequired>false</PromotionCodeRequired><SkipTargetingConditionOnProperPromotionCodeEntered>false</SkipTargetingConditionOnProperPromotionCodeEntered><CheckTargetingConditionAtRuntime>true</CheckTargetingConditionAtRuntime><PromotionCodeCondition impl=\"com.ibm.commerce.marketing.promotion.condition.PromotionCodeCondition\"/><Targeting impl=\"com.ibm.commerce.marketing.promotion.condition.TargetingCondition\"></Targeting><CustomConditions/><PurchaseCondition impl=\"com.ibm.commerce.marketing.promotion.condition.PurchaseCondition\"><Pattern impl=\"com.ibm.commerce.marketing.promotion.condition.Pattern\"><Constraint impl=\"com.ibm.commerce.marketing.promotion.condition.Constraint\"><WeightedRange impl=\"com.ibm.commerce.marketing.promotion.condition.WeightedRange\"><LowerBound>1</LowerBound><UpperBound>1</UpperBound><Weight>1</Weight></WeightedRange><FilterChain impl=\"com.ibm.commerce.marketing.promotion.condition.FilterChain\"><Filter impl=\"com.ibm.commerce.marketing.promotion.condition.MultiSKUFilter\"><IncludeCatEntryKey><CatalogEntryKey><SKU>"
                    + cpSKU
                    + "</SKU><DN>ou=asset store organization,o=extended sites organization,o=root organization</DN></CatalogEntryKey></IncludeCatEntryKey><IncludeCatEntryKey><CatalogEntryKey><SKU>"
                    + cpSKU
                    + "</SKU><DN>ou=asset store organization,o=extended sites organization,o=root organization</DN></CatalogEntryKey></IncludeCatEntryKey></Filter></FilterChain></Constraint></Pattern><Distribution impl=\"com.ibm.commerce.marketing.promotion.reward.Distribution\"><Type>Volume</Type><Base>Quantity</Base><Currency>GBP</Currency><Range impl=\"com.ibm.commerce.marketing.promotion.reward.DistributionRange\"><UpperBound>-1</UpperBound><LowerBound>0</LowerBound><UpperBoundIncluded>false</UpperBoundIncluded><LowerBoundIncluded>true</LowerBoundIncluded><RewardChoice><Reward impl=\"com.ibm.commerce.marketing.promotion.reward.DefaultReward\"><AdjustmentFunction impl=\"com.ibm.commerce.marketing.promotion.reward.AdjustmentFunction\"><FilterChain impl=\"com.ibm.commerce.marketing.promotion.condition.FilterChain\"><Filter impl=\"com.ibm.commerce.marketing.promotion.condition.DummyFilter\"/></FilterChain><Adjustment impl=\"com.ibm.commerce.marketing.promotion.reward.FixedAmountOffAdjustment\"><AmountOff>"
                    + amountOff
                    + "</AmountOff><Currency>GBP</Currency><AdjustmentType>IndividualAffectedItems</AdjustmentType></Adjustment></AdjustmentFunction></Reward></RewardChoice></Range><PatternFilter impl=\"com.ibm.commerce.marketing.promotion.condition.DummyPatternFilter\"/></Distribution></PurchaseCondition></Promotion>";
            }
            else if (productType.equals("P") && percentageOff!=null) {
                promotionXML = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Promotion impl=\"com.ibm.commerce.marketing.promotion.DefaultPromotion\"><PromotionKey><PromotionName>"+promotionName +"</PromotionName><StoreKey><DN>ou=asset store organization,o=extended sites organization,o=root organization</DN><Identifier>monitiseStorefrontAssetStore</Identifier></StoreKey><Version>0</Version><Revision>0</Revision></PromotionKey><PromotionGroupKey><GroupName>ProductLevelPromotion</GroupName><StoreKey><DN>ou=asset store organization,o=extended sites organization,o=root organization</DN><Identifier>monitiseStorefrontAssetStore</Identifier></StoreKey></PromotionGroupKey><TypedNLDescription impl=\"com.ibm.commerce.marketing.promotion.TypedNLDescription\"><DefaultLocale>en_US</DefaultLocale><Description locale=\"en_US\" type=\"admin\">"+promotionName +"</Description></TypedNLDescription><Priority>0</Priority><Exclusive>0</Exclusive><ExemptPolicyList/><ExplicitlyAppliedPolicyList/><Status>1</Status><LastUpdate>16-07-2014 09:13:08</LastUpdate><LastUpdateBy><CustomerKey><LogonId>wcsadmin</LogonId></CustomerKey></LastUpdateBy><PerOrderLimit>-1</PerOrderLimit><PerShopperLimit>-1</PerShopperLimit><ApplicationLimit>-1</ApplicationLimit><TargetSales>0</TargetSales><CorrespondingRBDTypeName>ProductLevelPerItemPercentDiscount</CorrespondingRBDTypeName><Schedule impl=\"com.ibm.commerce.marketing.promotion.schedule.PromotionSchedule\"><DateRange impl=\"com.ibm.commerce.marketing.promotion.schedule.DateRangeSchedule\"><Start inclusive=\"true\">"+fromDate+"</Start><End inclusive=\"true\">"+toDate+"</End></DateRange><TimeWithinADay impl=\"com.ibm.commerce.marketing.promotion.schedule.TimeRangeWithinADaySchedule\"><Start inclusive=\"true\">00:00:00</Start><End inclusive=\"true\">23:59:59</End></TimeWithinADay><Week impl=\"com.ibm.commerce.marketing.promotion.schedule.WeekDaySchedule\"><WeekDay>SUNDAY</WeekDay><WeekDay>MONDAY</WeekDay><WeekDay>TUESDAY</WeekDay><WeekDay>WEDNESDAY</WeekDay><WeekDay>THURSDAY</WeekDay><WeekDay>FRIDAY</WeekDay><WeekDay>SATURDAY</WeekDay></Week></Schedule><PromotionType>0</PromotionType><PromotionCodeRequired>false</PromotionCodeRequired><SkipTargetingConditionOnProperPromotionCodeEntered>false</SkipTargetingConditionOnProperPromotionCodeEntered><CheckTargetingConditionAtRuntime>true</CheckTargetingConditionAtRuntime><PromotionCodeCondition impl=\"com.ibm.commerce.marketing.promotion.condition.PromotionCodeCondition\"/><Targeting impl=\"com.ibm.commerce.marketing.promotion.condition.TargetingCondition\"></Targeting><CustomConditions/><PurchaseCondition impl=\"com.ibm.commerce.marketing.promotion.condition.PurchaseCondition\"><Pattern impl=\"com.ibm.commerce.marketing.promotion.condition.Pattern\"><Constraint impl=\"com.ibm.commerce.marketing.promotion.condition.Constraint\"><WeightedRange impl=\"com.ibm.commerce.marketing.promotion.condition.WeightedRange\"><LowerBound>1</LowerBound><UpperBound>1</UpperBound><Weight>1</Weight></WeightedRange><FilterChain impl=\"com.ibm.commerce.marketing.promotion.condition.FilterChain\"><Filter impl=\"com.ibm.commerce.marketing.promotion.condition.MultiSKUFilter\"><IncludeCatEntryKey><CatalogEntryKey><SKU>"
                    + cpSKU
                    + "</SKU><DN>ou=asset store organization,o=extended sites organization,o=root organization</DN></CatalogEntryKey></IncludeCatEntryKey><IncludeCatEntryKey><CatalogEntryKey><SKU>"
                    + cpSKU
                    + "</SKU><DN>ou=asset store organization,o=extended sites organization,o=root organization</DN></CatalogEntryKey></IncludeCatEntryKey></Filter></FilterChain></Constraint></Pattern><Distribution impl=\"com.ibm.commerce.marketing.promotion.reward.Distribution\"><Type>Volume</Type><Base>Quantity</Base><Currency>GBP</Currency><Range impl=\"com.ibm.commerce.marketing.promotion.reward.DistributionRange\"><UpperBound>-1</UpperBound><LowerBound>0</LowerBound><UpperBoundIncluded>false</UpperBoundIncluded><LowerBoundIncluded>true</LowerBoundIncluded><RewardChoice><Reward impl=\"com.ibm.commerce.marketing.promotion.reward.DefaultReward\"><AdjustmentFunction impl=\"com.ibm.commerce.marketing.promotion.reward.AdjustmentFunction\"><FilterChain impl=\"com.ibm.commerce.marketing.promotion.condition.FilterChain\"><Filter impl=\"com.ibm.commerce.marketing.promotion.condition.DummyFilter\"/></FilterChain><Adjustment impl=\"com.ibm.commerce.marketing.promotion.reward.FixedAmountOffAdjustment\"><AmountOff>"
                    + percentageOff
                    + "</AmountOff><Currency>GBP</Currency><AdjustmentType>IndividualAffectedItems</AdjustmentType></Adjustment></AdjustmentFunction></Reward></RewardChoice></Range><PatternFilter impl=\"com.ibm.commerce.marketing.promotion.condition.DummyPatternFilter\"/></Distribution></PurchaseCondition></Promotion>";                
            }
            else if (productType.equals("S") && amountOff!=null){
                promotionXML = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Promotion impl=\"com.ibm.commerce.marketing.promotion.DefaultPromotion\"><PromotionKey><PromotionName>"+promotionName +"</PromotionName><StoreKey><DN>ou=asset store organization,o=extended sites organization,o=root organization</DN><Identifier>monitiseStorefrontAssetStore</Identifier></StoreKey><Version>0</Version><Revision>0</Revision></PromotionKey><PromotionGroupKey><GroupName>ShippingPromotion</GroupName><StoreKey><DN>ou=asset store organization,o=extended sites organization,o=root organization</DN><Identifier>monitiseStorefrontAssetStore</Identifier></StoreKey></PromotionGroupKey><TypedNLDescription impl=\"com.ibm.commerce.marketing.promotion.TypedNLDescription\"><DefaultLocale>en_US</DefaultLocale><Description locale=\"en_US\" type=\"admin\">"+promotionName +"</Description></TypedNLDescription><Priority>0</Priority><Exclusive>0</Exclusive><ExemptPolicyList/><ExplicitlyAppliedPolicyList/><Status>1</Status><LastUpdate>16-07-2014 14:54:37</LastUpdate><LastUpdateBy><CustomerKey><LogonId>wcsadmin</LogonId></CustomerKey></LastUpdateBy><PerOrderLimit>-1</PerOrderLimit><PerShopperLimit>-1</PerShopperLimit><ApplicationLimit>-1</ApplicationLimit><TargetSales>0</TargetSales><CorrespondingRBDTypeName>OrderLevelFixedAmountOffShippingDiscount</CorrespondingRBDTypeName><Schedule impl=\"com.ibm.commerce.marketing.promotion.schedule.PromotionSchedule\"><DateRange impl=\"com.ibm.commerce.marketing.promotion.schedule.DateRangeSchedule\"><Start inclusive=\"true\">"+fromDate+"</Start><End inclusive=\"true\">"+toDate+"</End></DateRange><TimeWithinADay impl=\"com.ibm.commerce.marketing.promotion.schedule.TimeRangeWithinADaySchedule\"><Start inclusive=\"true\">00:00:00</Start><End inclusive=\"true\">23:59:59</End></TimeWithinADay><Week impl=\"com.ibm.commerce.marketing.promotion.schedule.WeekDaySchedule\"><WeekDay>SUNDAY</WeekDay><WeekDay>MONDAY</WeekDay><WeekDay>TUESDAY</WeekDay><WeekDay>WEDNESDAY</WeekDay><WeekDay>THURSDAY</WeekDay><WeekDay>FRIDAY</WeekDay><WeekDay>SATURDAY</WeekDay></Week></Schedule><PromotionType>0</PromotionType><PromotionCodeRequired>false</PromotionCodeRequired><SkipTargetingConditionOnProperPromotionCodeEntered>false</SkipTargetingConditionOnProperPromotionCodeEntered><CheckTargetingConditionAtRuntime>true</CheckTargetingConditionAtRuntime><PromotionCodeCondition impl=\"com.ibm.commerce.marketing.promotion.condition.PromotionCodeCondition\"/><Targeting impl=\"com.ibm.commerce.marketing.promotion.condition.TargetingCondition\"></Targeting><CustomConditions/><PurchaseCondition impl=\"com.ibm.commerce.marketing.promotion.condition.PurchaseCondition\"><Pattern impl=\"com.ibm.commerce.marketing.promotion.condition.Pattern\"><Constraint impl=\"com.ibm.commerce.marketing.promotion.condition.Constraint\"><WeightedRange impl=\"com.ibm.commerce.marketing.promotion.condition.WeightedRange\"><LowerBound>1</LowerBound><UpperBound>-1</UpperBound><Weight>1</Weight></WeightedRange><FilterChain impl=\"com.ibm.commerce.marketing.promotion.condition.FilterChain\"><Filter impl=\"com.ibm.commerce.marketing.promotion.condition.DummyFilter\"/></FilterChain></Constraint></Pattern><Distribution impl=\"com.ibm.commerce.marketing.promotion.reward.Distribution\"><Type>Volume</Type><Base>Cost</Base><Currency>USD</Currency><Range impl=\"com.ibm.commerce.marketing.promotion.reward.DistributionRange\"><UpperBound>-1</UpperBound><LowerBound>70</LowerBound><UpperBoundIncluded>true</UpperBoundIncluded><LowerBoundIncluded>true</LowerBoundIncluded><RewardChoice><Reward impl=\"com.ibm.commerce.marketing.promotion.reward.DefaultReward\"><AdjustmentFunction impl=\"com.ibm.commerce.marketing.promotion.reward.AdjustmentFunction\"><FilterChain impl=\"com.ibm.commerce.marketing.promotion.condition.FilterChain\"><Filter impl=\"com.ibm.commerce.marketing.promotion.condition.ShippingModeFilter\"><DN>ou=asset store organization,o=extended sites organization,o=root organization</DN><StoreIdentifier>monitiseStorefrontAssetStore</StoreIdentifier><Carrier>XYZ Carrier</Carrier><ShippingCode>US Regular Delivery</ShippingCode></Filter></FilterChain><Adjustment impl=\"com.ibm.commerce.marketing.promotion.reward.FixedCostShippingAdjustment\"><FixedCost>0</FixedCost><Currency>USD</Currency><AdjustmentType>IndividualAffectedItems</AdjustmentType></Adjustment></AdjustmentFunction></Reward></RewardChoice></Range><PatternFilter impl=\"com.ibm.commerce.marketing.promotion.condition.DummyPatternFilter\"/></Distribution></PurchaseCondition></Promotion>";
            }
            else if (productType.equals("S") && shipQualAmount!=null){
                promotionXML = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Promotion impl=\"com.ibm.commerce.marketing.promotion.DefaultPromotion\"><PromotionKey><PromotionName>"+promotionName +"</PromotionName><StoreKey><DN>ou=asset store organization,o=extended sites organization,o=root organization</DN><Identifier>monitiseStorefrontAssetStore</Identifier></StoreKey><Version>0</Version><Revision>0</Revision></PromotionKey><PromotionGroupKey><GroupName>ShippingPromotion</GroupName><StoreKey><DN>ou=asset store organization,o=extended sites organization,o=root organization</DN><Identifier>monitiseStorefrontAssetStore</Identifier></StoreKey></PromotionGroupKey><TypedNLDescription impl=\"com.ibm.commerce.marketing.promotion.TypedNLDescription\"><DefaultLocale>en_US</DefaultLocale><Description locale=\"en_US\" type=\"admin\">"+promotionName +"</Description></TypedNLDescription><Priority>0</Priority><Exclusive>0</Exclusive><ExemptPolicyList/><ExplicitlyAppliedPolicyList/><Status>1</Status><LastUpdate>16-07-2014 14:54:37</LastUpdate><LastUpdateBy><CustomerKey><LogonId>wcsadmin</LogonId></CustomerKey></LastUpdateBy><PerOrderLimit>-1</PerOrderLimit><PerShopperLimit>-1</PerShopperLimit><ApplicationLimit>-1</ApplicationLimit><TargetSales>0</TargetSales><CorrespondingRBDTypeName>OrderLevelFixedShippingDiscount</CorrespondingRBDTypeName><Schedule impl=\"com.ibm.commerce.marketing.promotion.schedule.PromotionSchedule\"><DateRange impl=\"com.ibm.commerce.marketing.promotion.schedule.DateRangeSchedule\"><Start inclusive=\"true\">"+fromDate+"</Start><End inclusive=\"true\">"+toDate+"</End></DateRange><TimeWithinADay impl=\"com.ibm.commerce.marketing.promotion.schedule.TimeRangeWithinADaySchedule\"><Start inclusive=\"true\">00:00:00</Start><End inclusive=\"true\">23:59:59</End></TimeWithinADay><Week impl=\"com.ibm.commerce.marketing.promotion.schedule.WeekDaySchedule\"><WeekDay>SUNDAY</WeekDay><WeekDay>MONDAY</WeekDay><WeekDay>TUESDAY</WeekDay><WeekDay>WEDNESDAY</WeekDay><WeekDay>THURSDAY</WeekDay><WeekDay>FRIDAY</WeekDay><WeekDay>SATURDAY</WeekDay></Week></Schedule><PromotionType>0</PromotionType><PromotionCodeRequired>false</PromotionCodeRequired><SkipTargetingConditionOnProperPromotionCodeEntered>false</SkipTargetingConditionOnProperPromotionCodeEntered><CheckTargetingConditionAtRuntime>true</CheckTargetingConditionAtRuntime><PromotionCodeCondition impl=\"com.ibm.commerce.marketing.promotion.condition.PromotionCodeCondition\"/><Targeting impl=\"com.ibm.commerce.marketing.promotion.condition.TargetingCondition\"></Targeting><CustomConditions/><PurchaseCondition impl=\"com.ibm.commerce.marketing.promotion.condition.PurchaseCondition\"><Pattern impl=\"com.ibm.commerce.marketing.promotion.condition.Pattern\"><Constraint impl=\"com.ibm.commerce.marketing.promotion.condition.Constraint\"><WeightedRange impl=\"com.ibm.commerce.marketing.promotion.condition.WeightedRange\"><LowerBound>1</LowerBound><UpperBound>-1</UpperBound><Weight>1</Weight></WeightedRange><FilterChain impl=\"com.ibm.commerce.marketing.promotion.condition.FilterChain\"><Filter impl=\"com.ibm.commerce.marketing.promotion.condition.DummyFilter\"/></FilterChain></Constraint></Pattern><Distribution impl=\"com.ibm.commerce.marketing.promotion.reward.Distribution\"><Type>Volume</Type><Base>Cost</Base><Currency>USD</Currency><Range impl=\"com.ibm.commerce.marketing.promotion.reward.DistributionRange\"><UpperBound>-1</UpperBound><LowerBound>70</LowerBound><UpperBoundIncluded>true</UpperBoundIncluded><LowerBoundIncluded>true</LowerBoundIncluded><RewardChoice><Reward impl=\"com.ibm.commerce.marketing.promotion.reward.DefaultReward\"><AdjustmentFunction impl=\"com.ibm.commerce.marketing.promotion.reward.AdjustmentFunction\"><FilterChain impl=\"com.ibm.commerce.marketing.promotion.condition.FilterChain\"><Filter impl=\"com.ibm.commerce.marketing.promotion.condition.ShippingModeFilter\"><DN>ou=asset store organization,o=extended sites organization,o=root organization</DN><StoreIdentifier>monitiseStorefrontAssetStore</StoreIdentifier><Carrier>XYZ Carrier</Carrier><ShippingCode>US Regular Delivery</ShippingCode></Filter></FilterChain><Adjustment impl=\"com.ibm.commerce.marketing.promotion.reward.FixedCostShippingAdjustment\"><FixedCost>0</FixedCost><Currency>USD</Currency><AdjustmentType>IndividualAffectedItems</AdjustmentType></Adjustment></AdjustmentFunction></Reward></RewardChoice></Range><PatternFilter impl=\"com.ibm.commerce.marketing.promotion.condition.DummyPatternFilter\"/></Distribution></PurchaseCondition></Promotion>";
            }
            
            
           String[] dataArray={promotionXML,promotionId,cpCode,cpSKU,productType,percentageOff,amountOff,shipQualAmount,promotionName};
           return dataArray;
        } catch (SQLException sqle) {
            throw sqle;
        } finally {
            if (rs != null) {
                rs.close();
                rs = null;
            }
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

    
    
    
}
