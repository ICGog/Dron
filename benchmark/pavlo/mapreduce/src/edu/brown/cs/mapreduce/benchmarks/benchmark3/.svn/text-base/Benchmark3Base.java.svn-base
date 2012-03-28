/**
 * 
 */
package edu.brown.cs.mapreduce.benchmarks.benchmark3;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;

import edu.brown.cs.mapreduce.BenchmarkBase;

/**
 * @author pavlo
 *
 */
public abstract class Benchmark3Base extends MapReduceBase {
	protected Boolean DEBUG = false;
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapred.MapReduceBase#configure(org.apache.hadoop.mapred.JobConf)
	 */
	@Override
	public void configure(JobConf job) {
        String property = job.get(BenchmarkBase.PROPERTY_DEBUG);
        if (property != null) {
        	this.DEBUG = Boolean.parseBoolean(property);
        
        }
		super.configure(job);
	}
	
}
