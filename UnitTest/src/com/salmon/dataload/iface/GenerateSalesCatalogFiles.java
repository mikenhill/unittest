package com.salmon.dataload.iface;

import static com.salmon.dataload.iface.DataLoadConstants.INVALID_ARGUMENTS;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.logging.Level;

import com.salmon.dataload.utils.UnicodeBOMInputStream;

/**
 * Abstract class that contains methods to allow the generation of the attributes file for the products and pricing interfaces.
 * 
 * @author Felipe Jauregui
 * @Date : 13 March 2011
 */

public class GenerateSalesCatalogFiles extends GenerateDMFiles {
    // static final String DELIMITER = "\\|";
    private static final int PRODUCT_CODE_INDEX = 5;
    private static final String CLASSNAME = GenerateSalesCatalogFiles.class.getName();
    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(CLASSNAME);
    private BufferedWriter out1;
    private BufferedWriter out2_1;
    private BufferedWriter out2_2;
    private BufferedWriter out3;
    private BufferedWriter out4;

    /**
     * This method will pass in the 5 file names that are to be created
     * 
     * @param salescatalogues the first forth file
     * @param sourceFileName the source file name
     * @param assetsalescataloguecategorie the first target file
     * @param assetsalescatalogueproducts the second target file
     * @param salescataloguecategoriestop the fifth target file
     * @param salescataloguecategories the third target file
     */

    GenerateSalesCatalogFiles(final String salescatalogues, final String sourceFileName, final String assetsalescataloguecategorie,
            final String assetsalescatalogueproducts1, final String assetsalescatalogueproducts2, final String salescataloguecategoriestop, final String salescataloguecategories) {

        super(salescatalogues, sourceFileName, assetsalescataloguecategorie, assetsalescatalogueproducts1, assetsalescatalogueproducts2, 
                salescataloguecategoriestop,
                salescataloguecategories);

    }

    /**
     * 
     * This method will create five out put files from the source file.
     * 
     * @throws FileNotFoundException
     * @throws IOException
     */
    private void processInputFile() throws IOException {

        OutputStreamWriter fstream1 = new FileWriter(getTargetFileName1());
        out1 = new BufferedWriter(fstream1);
        out1.write(GenerateDMFiles.TARGET_FILE1_HEADER);

        OutputStreamWriter fstream2_1 = new OutputStreamWriter(new FileOutputStream(getTargetFileName2_1()),"UTF-8");
        out2_1 = new BufferedWriter(fstream2_1);
        out2_1.write(TARGET_FILE2_1_HEADER);
        
        OutputStreamWriter fstream2_2 = new OutputStreamWriter(new FileOutputStream(getTargetFileName2_2()),"UTF-8");  
        out2_2 = new BufferedWriter(fstream2_2);
        out2_2.write(TARGET_FILE2_2_HEADER);
        
        OutputStreamWriter fstream3 = new OutputStreamWriter(new FileOutputStream(getTargetFileName3()),"UTF-8");
        out3 = new BufferedWriter(fstream3);
        out3.write(TARGET_FILE3_HEADER);
        
        OutputStreamWriter fstream4 = new OutputStreamWriter(new FileOutputStream(getTargetFileName4()),"UTF-8");
        out4 = new BufferedWriter(fstream4);
        out4.write(TARGET_FILE4_HEADER);

        FileInputStream fr = new FileInputStream(getSourceFileName2());
        InputStream cleanStream = new UnicodeBOMInputStream(fr).skipBOM();
        InputStreamReader in = new InputStreamReader(cleanStream, "UTF-8");
        BufferedReader br = new BufferedReader(in);
        
        String s;
        // int count = 0;
        ArrayList<String> existingLines = new ArrayList<String>();
        ArrayList<String> existingProduct = new ArrayList<String>();
        ArrayList<String> existingTopCategory = new ArrayList<String>();
        String groupIdentifierBottom = null;

        ArrayList<String> depots = getDepots();

        while ((s = br.readLine()) != null) {

            // Remove first split
            String fullName = s.substring(1, s.length());
            // Break down the name
            String parent = "";
            String[] categories = fullName.split("\\/");
            // Remove the last entry
            String currentPath = fullName.split("\\/")[0];
            String keyword = fullName.split("\\/")[2];
            String lastInPath = fullName.split("\\/")[categories.length - 1];
            String belowTheLineParentIdentifier = "";

            int topCounter = 0;

            for (String category : categories) {
                
                topCounter = topCounter + 1;
                // Set top categories
                if (topCounter == 1) {
                    //parent = category;
                    if (!existingTopCategory.contains(category)) {
                        existingTopCategory.add(category);
                        for (String depot : depots) {
                            // TopCatgroupIdentifier|CatalogueIdentifier|LinkSalesCatalogueIdentifier
                            String belowTheLineTopSalesCatalogGroup = category + "|" + depot + "|" + "0000";
                            // write to file salescataloguecategoriestop_INT_135
                            out3.write(belowTheLineTopSalesCatalogGroup + "\n");
                        }
                    }
                }
                    
                if (!lastInPath.equals(category)) {
                    
                    String groupIdentifier = (parent.equals("") ? currentPath : currentPath + "." + category);
                    category = (parent.equals("") ? groupIdentifier : category);

                    // GroupIdentifier|TopGroup|ParentGroupIdentifier|CatalogueIdentifier|Name|Shortdesc|LongDesc|Thumbnail|FullImage|Keyword|Delete
                    String newLine = groupIdentifier + "|" + (parent.equals("") ? "True" : "") + "|" + parent + "|" + "0000" + "|"
                            + (parent.equals("") ? groupIdentifier : category) + "|" + category + " Short Description" + "|" + category
                            + " Long Description" + "|" + "thumbnail/image.jpg" + "|" + "images/image.jpg" + "|" + keyword + "|" + 0;
                    currentPath = currentPath + (parent.equals("") ? "" : "." + category);
                    belowTheLineParentIdentifier = parent;
                    parent = currentPath;
                    // Check if it exsists and if not print
                    if (!existingLines.contains(groupIdentifier)) {
                        existingLines.add(groupIdentifier);
                        // System.out.println(newLine);
                        groupIdentifierBottom = groupIdentifier;

                        // for each below the line catalogue
                        for (String depot : depots) {
                            // GroupIdentifier|ParentGroupIdentifier|CatalogueIdentifier|LinkCatalogueIdentifier
                            String belowTheLineSalesCatalog = groupIdentifier + "|" + belowTheLineParentIdentifier + "|" + depot + "|" + "0000";
                            // writting to salescataloguecategories_INT_135
                           
                                out4.write(belowTheLineSalesCatalog + "\n");
                           
                            
                        }
                        // write to assetsalescataloguecategories_INT_135
                        out1.write(newLine + "\n");
                    }
                } else {
                    if (!existingProduct.contains(lastInPath.substring(PRODUCT_CODE_INDEX) + groupIdentifierBottom)) {
                        existingProduct.add(lastInPath.substring(PRODUCT_CODE_INDEX) + groupIdentifierBottom);
                        // ProductNumber|SalesCategoryIdentifier|CatalogueIdentifier
                        // write to file assetsalescatalogueproducts_INT_135
                        out2_1.write(lastInPath.substring(PRODUCT_CODE_INDEX) + "-P" + "|" + groupIdentifierBottom + "|" + "0000" + "\n");
                        out2_1.write(lastInPath.substring(PRODUCT_CODE_INDEX)        + "|" + groupIdentifierBottom + "|" + "0000" + "\n");
                        
                        for (String depot : depots) {
                            out2_2.write(lastInPath.substring(PRODUCT_CODE_INDEX) + "-P" + "|" + groupIdentifierBottom + "|" + depot + "\n");
                            out2_2.write(lastInPath.substring(PRODUCT_CODE_INDEX)        + "|" + groupIdentifierBottom + "|" + depot + "\n");
                        }
                        
                    }
                }
                
            }
        }
        out1.close();
        out2_1.close();
        out2_2.close();
        out3.close();
        out4.close();
    }

    /**
     * This method will load all the required depots.
     * 
     * @return String[]
     * @throws IOException
     */

    @SuppressWarnings("unchecked")
    private ArrayList<String> getDepots() throws IOException {

        FileReader salesCataloguesFile = new FileReader(getSourceFileName1());
        BufferedReader br = new BufferedReader(salesCataloguesFile);
        String s;
        ArrayList<String> depotList = new ArrayList();
        while ((s = br.readLine()) != null) {
            String depot = s.split("\\|")[0];
            if (depot.matches("^[-+]?\\d+(\\.\\d+)?$")) {
                depotList.add(depot);
            }
        }
        return depotList;
    }

    /**
     * This method run the application to generate the files
     * 
     * @param args the files that are to be created
     * @throws IOException
     */
    public static void main(final String[] args) throws IOException {

        if (args.length == 7 && args[0].length() > 0 && args[1].length() > 0 && args[2].length() > 0 && args[3].length() > 0 && args[4].length() > 0
                && args[5].length() > 0  && args[6].length() > 0) {
            LOGGER.logp(Level.INFO, CLASSNAME, "main", "Generating sales catalog files " + args[1] + " from salse file " + args[0]);
            GenerateSalesCatalogFiles generate = new GenerateSalesCatalogFiles(args[0], args[1], args[2], args[3], args[4], args[5], args[6]);
            generate.processInputFile();
        } else {
            LOGGER.logp(Level.SEVERE, CLASSNAME, "main", "Invalid arguments in GenerateSalesCatalogFiles");
            System.exit(INVALID_ARGUMENTS);
        }

    }

}
