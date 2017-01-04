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
 * Generates the Product Attributes interface file from the Product attirutes file. It takes the Identifier and Value Identifier columns from the
 * Products file and creates data rows in the Product Attributes file, copying these columns into the Identifier, and Allowed value columns
 * respectively.
 * 
 * @author Stephen Gair
 * @revision : 1.0
 * @Date : 19 December 2011
 */

public final class GenerateProductAttributesFile extends GenerateAttributesFile {

    private static final String CLASSNAME = GeneratePriceAttributesFile.class.getName();
    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(CLASSNAME);

    /**
     * Constructor
     * 
     * @param sourceFileName - the source file name
     * @param targetFileName - the target file name
     */
    private GenerateProductAttributesFile(final String sourceFileName, final String targetFileName) {
        super(sourceFileName, targetFileName);
    }

    /**
     * Two arguments must be required, the source product attributes file name, and the target product attributes file name.
     * 
     * @param args - input arguments
     */
    public static void main(final String[] args) {
        if (args.length == 2 && args[0].length() > 0 && args[1].length() > 0) {
            LOGGER.logp(Level.INFO, CLASSNAME, "main", "Generating attributes file " + args[1] + " from product file " + args[0]);
            GenerateProductAttributesFile generate = new GenerateProductAttributesFile(args[0], args[1]);
            generate.run();
        } else {
            LOGGER.logp(Level.SEVERE, CLASSNAME, "main", "Invalid arguments in GenerateProductAttributesFile");
            System.exit(INVALID_ARGUMENTS);
        }
    }

    /**
     * Read the source products load file and return an array list of Attributes which hold the AttributeIdentifier and ValueIdentifier values.
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
            Attribute a = new Attribute(cols[SourceFileHeaders.AttributeIdentifier.ordinal()], cols[SourceFileHeaders.ValueIdentifier.ordinal()]);
            attributes.add(a);
        }
        br.close();
        in.close();
        fstream.close();
        if (attributes.size() > 1) {
            attributes.remove(0); // remove header
        }
        LOGGER.exiting(CLASSNAME, methodName);
        return attributes;
    }

    /**
     * Enum representation of the source file header columns
     */
    private enum SourceFileHeaders {
        PartNumber, AttributeIdentifier, ValueIdentifier, Value, Usage, Delete;
    }
}
