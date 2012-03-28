/***************************************************************************
*   Copyright (C) 2009 by Andy Pavlo, Brown University                    *
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

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;

/**
 * @author pavlo
 *
 */
public class GenerateData {

   /**
    * @param args
    */
   public static void main(String[] args) {
      String directory        = args[0];
      long bytes_file_wanted  = Long.valueOf(args[1]);
      long bytes_total_wanted = Long.valueOf(args[2]);
      
      Configuration conf = new Configuration();
      conf.addResource(new Path(System.getenv("HADOOP_HOME") + "/conf/hadoop-default.xml"));
      conf.addResource(new Path(System.getenv("HADOOP_HOME") + "/conf/hadoop-site.xml"));
      FileSystem fs = null;
      try {
         fs = FileSystem.get(conf);
      } catch (Exception ex) {
         ex.printStackTrace();
         System.exit(-1);
      }
      System.out.println("fs.default.name: " + conf.get("fs.default.name"));
      //System.exit(-1);
      
      /*
      IGenerator generators[] = { new RankingGenerator() } ; //,
                                  //new UserVisitGenerator() };
      Class classes[]         = { Ranking.class }; 
                                  //UserVisit.class };

      for (int ctr = 0; ctr < generators.length; ctr++){
         IGenerator generator = generators[ctr];
         long bytes_total_written = 0;
         int file_idx = 0;
         long total_records = 0;
         while (bytes_total_written < bytes_total_wanted) {
            long bytes_file_written = 0;
            long record_ctr = 0;
            String class_name = classes[ctr].getSimpleName();
            try {
               //
               // Create a new file for this data type
               //
               String file = directory + "/" + class_name + "s/" + class_name + String.format("%05d", file_idx++);
               Path outFile = new Path(file);
               System.err.print("CREATE: " + outFile.toUri());
               FSDataOutputStream out = fs.create(outFile);
               
               while (bytes_file_written < bytes_file_wanted) {
                  String data = (classes[ctr].cast(generator.generate())).toString() + "\n";
                  //System.out.print("\t[" + bytes_file_written + "]");
                  //System.out.print(data);
                  out.write(data.getBytes());
                  bytes_file_written += data.getBytes().length;
                  record_ctr++;
               } // WHILE
               out.close();
               System.err.println(" [" + record_ctr + " records]");
            } catch (Exception ex) {
               ex.printStackTrace();
               System.exit(-1);
            }
            total_records += record_ctr;
            bytes_total_written += bytes_file_written;
         } // WHILE
         System.out.println("\nTOTAL: " + total_records);
      } // FOR
      */
   }
}
