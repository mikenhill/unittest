package com.salmon.dataload.iface;

import static com.salmon.dataload.iface.DataLoadConstants.INVALID_ARGUMENTS;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.logging.Level;

/**
 * Generates the Attributes interface file from the Pricing file. It takes the UnitPrice, MeasureOfUnitPrice and UnitPriceMeasure columns from the
 * Products file and creates data rows in the Attributes file.
 * 
 * @author Stephen Gair
 * @revision : 1.0
 * @Date : 19 December 2011
 */

public final class GeneratePriceAttributesFile extends GenerateAttributesFile {

    private static final String CLASSNAME = GeneratePriceAttributesFile.class.getName();
    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(CLASSNAME);
    private static final int HEADER_COLUMN_COUNT = 3;

    /**
     * Constructor
     * 
     * @param sourceFileName - the source file name
     * @param targetFileName - the target file name
     */
    private GeneratePriceAttributesFile(final String sourceFileName, final String targetFileName) {
        super(sourceFileName, targetFileName);
    }

    /**
     * Two arguments must be required, the source (pricing) file name, and the target attributes file name.
     * 
     * @param args - input arguments
     */
    public static void main(final String[] args) {
        if (args.length == 2 && args[0].length() > 0 && args[1].length() > 0) {
            LOGGER.logp(Level.INFO, CLASSNAME, "main", "Generating attributes file " + args[1] + " from price file " + args[0]);
            GeneratePriceAttributesFile generate = new GeneratePriceAttributesFile(args[0], args[1]);
            generate.run();
        } else {
            LOGGER.logp(Level.SEVERE, CLASSNAME, "main", "Invalid arguments in GeneratePriceAttributesFile");
            System.exit(INVALID_ARGUMENTS);
        }
        LOGGER.logp(Level.INFO, CLASSNAME, "main", "Generating attributes file complete");
    }

    /**
     * Read the source input file and return an array list of Attributes which hold the identifier and values.
     * 
     * @return an array list representation of the input file.
     * @throws IOException
     */
    @Override
    ArrayList<Attribute> readInputFile() throws IOException {
        final String methodName = "readInputFile";
        LOGGER.entering(CLASSNAME, methodName, getSourceFileName());
        FileInputStream fstream = new FileInputStream(getSourceFileName());
        DataInputStream in = new DataInputStream(fstream);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String strLine;
        ArrayList<Attribute> attributes = new ArrayList<Attribute>();
        while ((strLine = br.readLine()) != null) {
            String[] cols = strLine.split(DELIMITER);
            Attribute unitPrice = new Attribute(SourceFileHeaders.UnitPrice.name(), cols[SourceFileHeaders.UnitPrice.ordinal()]);
            Attribute unitPriceMeasureAmount = new Attribute(SourceFileHeaders.UnitPriceMeasureAmount.name(), cols[SourceFileHeaders.UnitPriceMeasureAmount
                    .ordinal()]);
            Attribute unitPriceMeasure = new Attribute(SourceFileHeaders.UnitPriceMeasure.name(), cols[SourceFileHeaders.UnitPriceMeasure.ordinal()]);
            attributes.add(unitPrice);
            attributes.add(unitPriceMeasureAmount);
            attributes.add(unitPriceMeasure);
        }
        br.close();
        in.close();
        fstream.close();
        if (attributes.size() > HEADER_COLUMN_COUNT) {
            attributes.remove(0); // remove UnitPrice header
            attributes.remove(0); // remove UnitPriceMeasureAmount header
            attributes.remove(0); // remove UnitPriceMeasure header
        }
        LOGGER.exiting(CLASSNAME, methodName);
        return attributes;
    }

    /**
     * Enum representation of the source file header columns
     */
    private enum SourceFileHeaders {
          PartNumber, RetailPrice, Store, UnitPrice, UnitPriceMeasureAmount, UnitPriceMeasure, Delete
    };
}
