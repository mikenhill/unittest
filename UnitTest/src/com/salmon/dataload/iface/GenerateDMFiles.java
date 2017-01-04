package com.salmon.dataload.iface;

import java.io.StringWriter;
import java.io.Writer;
import java.util.logging.Level;

/**
 * Abstract class that contains methods to allow the generation of the attributes file for the sales catalogue interfaces.
 * 
 * @author Vincent White
 * @Date : 29 February 2012
 */

public abstract class GenerateDMFiles {

    static final String TARGET_FILE1_HEADER = "GroupIdentifier|TopGroup|ParentGroupIdentifier|CatalogueIdentifier|Name|Shortdesc|LongDesc|Thumbnail|"
            + "FullImage|Keyword|Delete" + "\n";
    static final String TARGET_FILE2_1_HEADER = "PartNumber|GroupIdentifier|CatalogueIdentifier|Delete" + "\n";
    
    static final String TARGET_FILE2_2_HEADER = "PartNumber|GroupIdentifier|CatalogueIdentifier|Delete" + "\n";
    
    static final String TARGET_FILE3_HEADER = "TopCatgroupIdentifier|CatalogueIdentifier|LinkCatalogueIdentifier|Delete" + "\n";
    static final String TARGET_FILE4_HEADER = "GroupIdentifier|ParentGroupIdentifier|CatalogueIdentifier|LinkCatalogueIdentifier|Delete" + "\n";

    static final String DELIMITER = "\\|";

    private final String sourceFileName1;
    private final String sourceFileName2;
    private final String targetFileName1;
    private final String targetFileName2_1;
    private final String targetFileName2_2;
    private final String targetFileName3;
    private final String targetFileName4;
    private static final String CLASSNAME = GenerateDMFiles.class.getName();
    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(CLASSNAME);

    /**
     * This is the constructor, it take a source file and five out put file names.
     * 
     * @param sourceFileName1 Sales Catalogues
     * @param sourceFileName2 BM Sales Hierarchy with Porducts
     * @param targetFileName1 assetsalescataloguecategories
     * @param targetFileName2 assetsalescatalogueproducts
     * @param targetFileName3 salescataloguecategoriestop
     * @param targetFileName4 salescataloguecategories
     */

    GenerateDMFiles(final String sourceFileName1, final String sourceFileName2, final String targetFileName1, 
            final String targetFileName2_1, final String targetFileName2_2,
            final String targetFileName3, final String targetFileName4) {
        this.sourceFileName1 = sourceFileName1;
        this.sourceFileName2 = sourceFileName2;
        this.targetFileName1 = targetFileName1;
        this.targetFileName2_1 = targetFileName2_1;
        this.targetFileName2_2 = targetFileName2_2;
        this.targetFileName3 = targetFileName3;
        this.targetFileName4 = targetFileName4;
    }

    /**
     * Log the Stack Trace
     * 
     * @param aThrowable - the throwable.
     * @param methodName - the method name.
     */
    @SuppressWarnings("unused")
    private static void logStackTrace(final Throwable aThrowable, final String methodName) {
        Writer result = new StringWriter();
        LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, result.toString());
    }

    String getSourceFileName1() {
        return sourceFileName1;
    }

    String getSourceFileName2() {
        return sourceFileName2;
    }

    public String getTargetFileName1() {
        return targetFileName1;
    }

    public String getTargetFileName2_1() {
        return targetFileName2_1;
    }

    public String getTargetFileName2_2() {
        return targetFileName2_2;
    }    
    
    public String getTargetFileName3() {
        return targetFileName3;
    }

    public String getTargetFileName4() {
        return targetFileName4;
    }

}
