/***************************************************************************
*   Copyright (C) 2008 by Andy Pavlo, Brown University                    *
*   http://www.cs.brown.edu/~pavlo/                                       *
*                                                                         *
*   Permission is hereby granted, free of charge, to any person obtaining *
*   a copy of this software and associated documentation files (the       *
*   "Software"), to deal in the Software without restriction, including   *
*   without limitation the rights to use, copy, modify, merge, publish,   *
*   distribute, sublicense, and/or sell copies of the Software, and to    *
*   permit persons to whom the Software is furnished to do so, subject to *
*   the following conditions:                                             *
*                                                                         *
*   The above copyright notice and this permission notice shall be        *
*   included in all copies or substantial portions of the Software.       *
*                                                                         *
*   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,       *
*   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF    *
*   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.*
*   IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR     *
*   OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, *
*   ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR *
*   OTHER DEALINGS IN THE SOFTWARE.                                       *
***************************************************************************/
package edu.brown.cs.mapreduce.generator;

import java.io.*;
import java.util.*;

import edu.brown.cs.mapreduce.*;
import edu.brown.cs.mapreduce.tuples.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;

/**
 * @author pavlo
 *
 */
public class DataLoader extends AbstractHadoopClient {
   public static final String VALID_TYPES[] = { "urls", "rankings", "uservisits", "sortgrep", "convert" };
   
   public static Boolean compress = false;
   public static Boolean sequence = false;
   public static Boolean tuple = false;
   public static Boolean local = false;
   public static Boolean xargs = false;
   public static Boolean debug = false;
   public static Integer limit = null;
   
   /**
    * @param args
    */
   public static void main(String[] args) {
      List<String> otherArgs = new ArrayList<String>();
      for (int i = 0; i < args.length; i++) {
         if ("-compress".equals(args[i])) {
            DataLoader.compress = true;
            DataLoader.sequence = true;
         } else if ("-sequence".equals(args[i])) {
            DataLoader.sequence = true;
         } else if ("-tuple".equals(args[i])) {
             DataLoader.tuple = true;
         } else if ("-local".equals(args[i])) {
            DataLoader.local = true;
         } else if ("-limit".equals(args[i])) {
            DataLoader.limit = Integer.parseInt(args[++i]);
         } else if ("-xargs".equals(args[i])) {
             DataLoader.xargs = true;
         } else if ("-debug".equals(args[i])) {
            DataLoader.debug = true;
         } else {
            otherArgs.add(args[i]);
         }
      } // FOR
      
      if (otherArgs.size() < 3 && !DataLoader.xargs) {
         System.err.println("USAGE: DataLoader <input type> <input file> <output file>");
         System.exit(1);
      }
      
      String input_type = otherArgs.get(0).toLowerCase();
      String input_file = otherArgs.get(1);
      String output_file = null;
      if (DataLoader.xargs) {
    	  output_file = input_file + ".dl";
      } else {
    	  output_file = otherArgs.get(2);
      }
      
      boolean valid = false;
      for (String type : DataLoader.VALID_TYPES) {
         if (type.equals(input_type)) {
            valid = true;
            break;
         }
      }
      if (!valid) {
         System.err.println("ERROR: Invalid input data type '" + input_type + "'");
         System.exit(1);
      }
      
      if (debug) {
    	  System.out.println("Input Type:  " + input_type);
    	  System.out.println("Input File:  " + input_file);
    	  System.out.println("Output File: " + output_file);
    	  System.out.println("Limit:       " + DataLoader.limit);
    	  System.out.println("Local:       " + DataLoader.local);
    	  System.out.println("XArgs:       " + DataLoader.xargs);
      }
      
      //
      // Get HDFS filesystem object that we can use for writing
      //
      FileSystem fs = null;
      Configuration conf = null;
      if (!DataLoader.local) {
         conf = AbstractHadoopClient.getConfiguration();
         try {
            fs = FileSystem.get(conf);
         } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(-1);
         }
         if (debug) System.out.println("fs.default.name: " + conf.get("fs.default.name"));
      }
      
      //
      // Now open the file that we want to read and start writing the contents to our file system
      // For some things, like 'urls' we will want reverse the order so that the data makes sense
      // in our key->value paradigm
      //
      BufferedReader in = null;
      DataOutputStream out = null;
      SequenceFile.Writer writer = null;
      int lines = 0;
      try {
         if (input_file.equals("-")) {
            in = new BufferedReader(new InputStreamReader(System.in));
         } else {
            in = new BufferedReader(new FileReader(input_file));
         }
      } catch (FileNotFoundException ex) {
         System.err.println("ERROR: The input file '" + input_file + "' was not found : " + ex.getMessage());
         System.exit(1);
      }
      try {
         if (!DataLoader.local) {
            //
            // FileSystem Writer
            //
            if (!DataLoader.sequence) {
               out = fs.create(new Path(output_file));
            //
            // SequenceFile Writer
            //
            } else {
               if (input_type.equals("sortgrep")) DataLoader.tuple = false;
               if (DataLoader.debug) System.out.print("Creating " + (DataLoader.compress ? "compressed " : "") + "SequenceFile.Writer for '" + output_file + "': ");
               Class<? extends Writable> key_class = Text.class;
               Class<? extends Writable> value_class = null;
               if (DataLoader.tuple) {
            	   if (input_type.equals("uservisits")) value_class = UserVisitsTuple.class;
            	   if (input_type.equals("rankings")) value_class = RankingsTuple.class;
               } else {
            	   value_class = Text.class;
               }
               writer = SequenceFile.createWriter(fs, conf, new Path(output_file), key_class, value_class, (DataLoader.compress ? SequenceFile.CompressionType.BLOCK : SequenceFile.CompressionType.NONE));
               if (DataLoader.debug) System.out.println("DONE!");
            }
         //
         // Local Filesystem
         //
         } else {
            out = new DataOutputStream(new FileOutputStream(output_file, true));   
         }
      } catch (IOException ex) {
         System.err.println("ERROR: Failed to open output file '" + output_file + "' : " + ex.getMessage());
         System.exit(1);
      }
      try {
         //
         // Now read in each line of the input file and append it to our output
         //
         while (in.ready()) {
            //
            // Ignore any misformated lines
            //
            String line = null;
            String key = "";
            String value = "";
            try {
               line = in.readLine();
               String data[] = line.split("\\" + BenchmarkBase.VALUE_DELIMITER);
               //
               // Switch the two values in a rankings record
               //
               if (input_type.equals("rankings")) {
                  key = data[1];
                  value = data[0];
                  for (int i = 2; i < data.length; i++) {
                     value += BenchmarkBase.VALUE_DELIMITER + data[i];
                  } // FOR
               //
               // Change the comma to a tab
               //
               } else if (input_type.equals("convert") || input_type.equals("uservisits")) {
                  key = data[0];
                  for (int i = 1; i < data.length; i++) {
                     if (i != 1) value += BenchmarkBase.VALUE_DELIMITER;
                     value += data[i];
                  } // FOR
               //
               // Don't do anything with the SortGrep data!
               //
               } else if (input_type.equals("sortgrep")) {
                  key = line.substring(0, 10);
                  value = line.substring(10);
               //
               // All others need to switch the first VALUE_DELIMITER to a KEYVALUE_DELIMITER
               //   
               } else {
                  line = line.replaceFirst(BenchmarkBase.VALUE_DELIMITER, BenchmarkBase.KEYVALUE_DELIMITER);
               }
               if (DataLoader.local || !DataLoader.sequence) {
                  line = key + BenchmarkBase.KEYVALUE_DELIMITER + value + "\n";
                  out.write(line.getBytes());
               } else {
                  //if (DataLoader.debug) System.out.println("[" + lines + "] " + key + " => " + value);
            	   if (DataLoader.tuple) {
            		   try {
	            		   data = value.split("\\" + BenchmarkBase.VALUE_DELIMITER);
	            		   Writable tuple_values[] = new Writable[data.length];
	            		   Class<?> types[] = (input_type.equals("uservisits") ? BenchmarkBase.USERVISITS_TYPES :BenchmarkBase.RANKINGS_TYPES); 
	            		   for (int ctr = 0; ctr < data.length; ctr++) {
	            			   //
	            			   // Important! You have to subtract one from the types list
	            			   // because the first one is really the key, but we're creating a tuple
	            			   // on just the values!!
	            			   //
	            			   if (types[ctr+1] == Text.class) {
	            				   tuple_values[ctr] = new Text(data[ctr]);
	            			   } else if (types[ctr+1] == IntWritable.class) {
	            				   tuple_values[ctr] = new IntWritable(Integer.valueOf(data[ctr]));
	            			   } else if (types[ctr+1] == DoubleWritable.class) {
	            				   tuple_values[ctr] = new DoubleWritable(Double.valueOf(data[ctr]));
	            			   } else if (types[ctr+1] == LongWritable.class) {
	            				   tuple_values[ctr] = new LongWritable(Long.valueOf(data[ctr]));
	            			   } else if (types[ctr+1] == FloatWritable.class) {
	            				   tuple_values[ctr] = new FloatWritable(Float.valueOf(data[ctr]));
	            			   } else {
	            				   System.err.println("Unsupported Class: " + types[ctr+1]);
	            				   System.exit(1);
	            			   }
	            			   if (DataLoader.debug) System.out.println("tuple_values[" + ctr + "] = " + tuple_values[ctr]);
	            		   }
	            		   AbstractTuple tuple = (input_type.equals("uservisits") ? new UserVisitsTuple(tuple_values) : new RankingsTuple(tuple_values));
	            		   if (DataLoader.debug) System.out.println("STORING TUPLE: " + tuple + " (DATA " + data + " | VALUE " + value + ")");
	            		   writer.append(new Text(key), tuple);
            		   } catch (Exception ex) {
            			   ex.printStackTrace();
                           System.err.println("Error[" + output_file + "]");
                           System.err.println("## Line:    " + lines);
                           System.err.println("## Content: " + line);
            		   }
            	   } else {
            		   writer.append(new Text(key), new Text(value));
            	   }
               }
               lines++;
               if (DataLoader.limit != null && lines >= DataLoader.limit) break;
               if (DataLoader.debug && lines % 1000000 == 0) System.out.println("\tWrote " + lines + " '" + input_type + "' records to '" + output_file + "'");
            } catch (Exception ex) {
               System.err.println("Error[" + output_file + "]");
               System.err.println("## Line:    " + lines);
               System.err.println("## Content: " + line);
               ex.printStackTrace();
               System.exit(1);
            }
         } // WHILE
      } catch (Exception ex) {
         ex.printStackTrace();
         System.exit(1);
      } finally {
         try {
            if (in != null) in.close();
            if (out != null) out.close();
            if (writer != null) writer.close();
         } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
         }
      }
      System.out.println("Wrote " + lines + " '" + input_type + "' records to '" + output_file + "'");
   }

}
