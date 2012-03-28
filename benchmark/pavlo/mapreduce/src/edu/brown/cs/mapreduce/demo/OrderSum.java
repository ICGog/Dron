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
package edu.brown.cs.mapreduce.demo;

import java.io.IOException;
import java.text.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/**
 * @author pavlo
 *
 */
public class OrderSum extends Configured implements Tool {

   /**
    * Mapper Object
    */
   public static class OrderSumMapper
                        extends MapReduceBase
                        implements Mapper<Text, Text, Text, DoubleWritable> {
      //
      // The date we are going to look for in our records
      //
      private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
      private String search_date_str = "2008-01-01";
      private Date search_date;
      {
         try {
            this.search_date = this.dateFormat.parse(this.search_date_str);
         } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
         }
      }
  
      public void map(Text key, Text value,
                      OutputCollector<Text, DoubleWritable> output,
                      Reporter reporter) throws IOException {
         Date key_date = null;
         try {
            key_date = this.dateFormat.parse(key.toString());
            if (key_date.compareTo(this.search_date) >= 0) {
               double d_value = Double.parseDouble(value.toString()); 
               output.collect(key, new DoubleWritable(d_value));
            }
         } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
         }
      }
      
      /*
      @Override
      public void configure(JobConf job) {
         super.configure(job);
         String param = job.getStrings("edu.brown.cs.pavlo.search_date")[0];
         if (param != null) {
            this.search_date_str = param;
            try {
               this.search_date = this.dateFormat.parse(this.search_date_str);
            } catch (Exception ex) {
               ex.printStackTrace();
               System.exit(1);
            }
         }
      }
      */
   }
   
   /**
    * Reducer Object
    */
   public static class OrderSumReducer
                        extends MapReduceBase
                        implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
     
      public void reduce(Text key, Iterator<DoubleWritable> values,
                        OutputCollector<Text, DoubleWritable> output, 
                        Reporter reporter) throws IOException {
         double sum = 0.00;
         while (values.hasNext()) {
            sum += values.next().get();
         } // WHILE
         output.collect(key, new DoubleWritable(sum));
      }
   }
   
   
   /**
    * The main driver for word count map/reduce program.
    * Invoke this method to submit the map/reduce job.
    * @throws IOException When there is communication problems with the job tracker.
    */
   public int run(String[] args) throws Exception {
      JobConf conf = new JobConf(this.getConf(), OrderSum.class);
      conf.setJobName(OrderSum.class.getSimpleName());
      
      // Input File Format
      conf.setInputFormat(KeyValueTextInputFormat.class);
      
      // Output Key/Value Types
      conf.setOutputKeyClass(Text.class);
      conf.setOutputValueClass(DoubleWritable.class);
      
      // Map/Reduce Classes
      conf.setMapperClass(OrderSum.OrderSumMapper.class);        
      conf.setReducerClass(OrderSum.OrderSumReducer.class);
      
      // Input/Output Paths (HDFS)
      FileInputFormat.setInputPaths(conf, "/demo/input/");
      FileOutputFormat.setOutputPath(conf, new Path("/demo/output/"));
      
      /***** Additional Features *****/
      // Compression
      //conf.setCompressMapOutput(true);
      
      // Combine
      //conf.setCombinerClass(OrderSum.OrderSumReducer.class);
      
      // Create a single output file
      conf.setNumReduceTasks(1);
      
      // Pass search date on command-line
      /* uncomment configure!
      if (args.length == 1) {
         conf.set("edu.brown.cs.pavlo.search_date", args[0]);
      }*/
      
      // Bombs away!
      JobClient.runJob(conf);
      
      return 0;
   }
   
   /**
    * @param args
    */
   public static void main(String[] args) throws Exception {
      int res = ToolRunner.run(new Configuration(), new OrderSum(), args);
      System.exit(res);
   }
}
