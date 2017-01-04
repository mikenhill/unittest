/*
PSVWriter write PSV File

Must be combined with your own code.

*/
package com.salmon.dataload.iface;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Write PSV (Pipe Separated Value) files. This format is used my Microsoft Word and Excel. Fields are separated by
 * commas, and enclosed in quotes if they contain commas or quotes. Embedded quotes are doubled. Embedded spaces do not
 * normally require surrounding quotes. The last field on the line is not followed by a pipe. Null fields are
 * represented by two pipe in a row.
 *
 * created with Intellij Idea
 *
 * @author copyright (c) 2002-2009 Roedy Green, Canadian Mind Products version
 * @version 2.9 2000-03-27 refactor using enums, support comments.
 */

public final class PSVWriter
    {
    // ------------------------------ FIELDS ------------------------------

    /**
     * true if want debugging output
     */
    static final boolean DEBUGGING = false;

    /**
     * line separator to use. We use Windows style for all platforms since psv is a Windows format file.
     */
    private static final String lineSeparator = System.getProperty("line.separator");

    /**
     * PrintWriter where PSV fields will be written.
     */
    private PrintWriter pw;

    /**
     * true if first field on line is ""
     */
    private boolean loneEmptyField;

    /**
     * true if write should trim lead/trail whitespace from fields before writing them.
     */
    private final boolean trim;

    /**
     * true if there has was a field previously written to this line, meaning there is a comma pending to be written.
     */
    private boolean wasPreviousField = false;

    /**
     * char to mark the start of a comment, usually ; or #
     */
    private char commentChar;

    /**
     * quote character, usually '\"' '\'' for SOL used to enclose fields containing a separator character.
     */
    private char quoteChar;

    /**
     * field separator character, usually ',' in North America, ';' in Europe and sometimes '\t' for tab.
     */
    private char separatorChar;

    /**
     * how much extra quoting you want
     */
    private int quoteLevel;

    // -------------------------- PUBLIC INSTANCE  METHODS --------------------------
    /**
     * convenience Constructor, defaults to quotelevel 1, comma separator , trim
     *
     * @param pw Buffered PrintWriter where fields will be written
     */
    public PSVWriter( PrintWriter pw )
        {
        // writer, quoteLevel, separatorChar, quoteChar, commentChar, trim
        this( pw, 1, '|', '\"', '#', true );
        }

    /**
     * Constructor
     *
     * @param pw            Buffered PrintWriter where fields will be written
     * @param quoteLevel    0 = minimal quotes, only aroud fields containing quotes or separators.
     *                      1 = quotes also around fields containing spaces.
     *                      2 = quotes around all fields, whether or not they contain commas, quotes or spaces.
     * @param separatorChar field separator character, usually ',' in North America, ';' in Europe and sometimes '\t' for
     *                      tab.  Note this is a 'char' not a "string".
     * @param quoteChar     char to use to enclose fields containing a separator, usually '\"'. Use (char)0 if
     *                      you don't want a quote character. Note this is a 'char' not a "string".
     * @param commentChar   char to prepend on any comments you write.  usually ; or #. Note this is a 'char' not a "string".
     * @param trim          true if writer should trim leading/trailing whitespace (e.g. blank, cr, Lf, tab) before writing
     *                      the field.
     */
    public PSVWriter( PrintWriter pw,
                      int quoteLevel,
                      char separatorChar,
                      char quoteChar,
                      char commentChar,
                      boolean trim )
        {
        this.pw = pw;
        this.quoteLevel = quoteLevel;
        this.separatorChar = separatorChar;
        this.quoteChar = quoteChar;
        this.commentChar = commentChar;
        this.trim = trim;
        }

    /**
     * Close the PrintWriter.
     */
    public void close()
        {
        if ( pw != null )
            {
            pw.close();
            pw = null;
            }
        }

    /**
     * Write a new line in the PSV output file to demark the end of record.
     */
    public void nl()
        {
        if ( pw == null )
            {
            throw new IllegalArgumentException(
                    "attempt to write to a closed PSVWriter" );
            }
        /* don't write last pending comma on the line */
        if ( loneEmptyField )
            {
            // single empty field on line, currently displayed as 0 chars
            pw.write( quoteChar );
            pw.write( quoteChar );
            }
        pw.write( lineSeparator );
        wasPreviousField = false;
        loneEmptyField = false;
        }

    /**
     * Write a comment followed by new line in the CVS output file to demark the end of record.
     *
     * @param comment comment string containing any chars.  Lead comment character will be applied automatically.
     */
    private void nl( final String comment )
        {
        if ( pw == null )
            {
            throw new IllegalArgumentException(
                    "attempt to write to a closed PSVWriter" );
            }
        if ( wasPreviousField )
            {
            if ( loneEmptyField )
                {
                // single empty field on line, currently displayed as 0 chars
                pw.write( quoteChar );
                pw.write( quoteChar );
                }
            // no comma, just extra space.
            pw.write( ' ' );
            }
        pw.write( commentChar );  // start comment with space # space
        pw.write( ' ' );
        pw.write( comment.trim() );
        pw.write( lineSeparator );
        wasPreviousField = false;
        loneEmptyField = false;
        }

    /**
     * Write one psv field to the file, followed by a separator unless it is the last field on the line. Lead and
     * trailing blanks will be removed.
     *
     * @param i The int to write. Any additional quotes or embedded quotes will be provided by put.
     */
    private void put( int i )
        {
        put( Integer.toString( i ) );
        }

    /**
     * Write one psv field to the file, followed by a separator unless it is the last field on the line. Lead and
     * trailing blanks will be removed.
     *
     * @param c The char to write. Any additional quotes or embedded quotes will be provided by put.
     */
    private void put( char c )
        {
        put( String.valueOf( c ) );
        }

    /**
     * Write one psv field to the file, followed by a separator unless it is the last field on the line. Lead and
     * trailing blanks will be removed.
     *
     * @param l The long to write. Any additional quotes or embedded quotes will be provided by put.
     */
    private void put( long l )
        {
        put( Long.toString( l ) );
        }

    /**
     * Write one psv field to the file, followed by a separator unless it is the last field on the line. Lead and
     * trailing blanks will be removed.
     *
     * @param d The double to write. Any additional quotes or embedded quotes will be provided by put.
     */
    private void put( double d )
        {
        put( Double.toString( d ) );
        }

    /**
     * Write one psv field to the file, followed by a separator unless it is the last field on the line. Lead and
     * trailing blanks will be removed.
     *
     * @param f The float to write. Any additional quotes or embedded quotes will be provided by put.
     */
    private void put( float f )
        {
        put( Float.toString( f ) );
        }

    /**
     * Write one psv field to the file, followed by a separator unless it is the last field on the line. Lead and
     * trailing blanks will be removed.   Don't use this method to write comments.
     *
     * @param s The string to write. Any additional quotes or embedded quotes will be provided by put. Null means start
     *          a new line.
     *
     * @see #nl(String)
     */
    public void put( String s )
        {
        
        if ( pw == null )
            {
            throw new IllegalArgumentException(
                    "attempt to write to a closed PSVWriter" );
            }
        if ( s == null )
            {
            nl();
            return;
            }

        if ( trim )
            {
            s = s.trim();
            }
        if ( wasPreviousField )
            {
            pw.write( separatorChar );
            loneEmptyField = false;
            }
        else
            {
            // first field on line
            loneEmptyField = s.trim().length() == 0;
            }
        if ( s.indexOf( quoteChar ) >= 0 )
            {
            /* worst case, needs surrounding quotes and internal quotes doubled */
            pw.write( quoteChar );
            for ( int i = 0, n = s.length(); i < n; i++ )
                {
                char c = s.charAt( i );
                if ( c == quoteChar )
                    {
                    pw.write( quoteChar );
                    pw.write( quoteChar );
                    }
                else
                    {
                    pw.write( c );
                    }
                }
            pw.write( quoteChar );
            }
        else if ( quoteLevel == 2
                  || quoteLevel == 1 && s.indexOf( ' ' ) >= 0
                  || s.indexOf( separatorChar ) >= 0
                  || s.indexOf( commentChar ) >= 0 )
            {
            /* need surrounding quotes */
            pw.write( quoteChar );
            pw.write( s );
            pw.write( quoteChar );
            }
        else
            {
            /* ordinary case, no surrounding quotes needed */
            pw.write( s );
            }
        /* make a note to print trailing comma later */
        wasPreviousField = true;
        }

    // --------------------------- main() method ---------------------------

    /**
     * Test driver
     *
     * @param args not used
     */
    public static void main( String[] args )
        {
        if ( DEBUGGING )
            {
            try
                {
                // write out a test file
                // writer, quoteLevel, separatorChar, quoteChar, commentChar, trim
                PSVWriter psv =
                        new PSVWriter( new PrintWriter( new BufferedWriter( new FileWriter( "C:\\temp.txt" ) ) ),
                                1,
                                ';',
                                '|',
                                '#',
                                true );
                psv.put( "abc" );
                psv.put( "def" );
                psv.put( "g h i" );
                psv.put( "jk,l" );
                psv.put( "m\"n\'o " );
                psv.nl();
                psv.put( "m\"n\'o " );
                psv.put( "    " );
                psv.put( "a" );
                psv.put( "x,y,z" );
                psv.put( "x;y;z" );
                psv.nl( "a comment" );
                psv.close();
                }
            catch ( IOException e )
                {
                e.printStackTrace();
                System.out.println( e.getMessage() );
                }
            }// end if
        }// end main
    }// end PSVWriter class.
