import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class LogFileAnalyzer {

  // **************** MAPPER ************************************************************
  public static class Map 
            extends Mapper<LongWritable, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1); // type of output value
    private Text word = new Text("NONE");                      // type of output key
  
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String [] myArr = value.toString().split(",");
      try {
    	  if (myArr.length >= 2) {
	    	  word.set(myArr[1]);          // identify by IP addresses
    		  context.write(word, one);    // create a pair <keyword, 1>
    	  }
      } catch(Exception e) {
          System.out.println("MAP: "+e.toString());    	  
      } finally {
    	  // do nothing...
      } 
    }
  }

  
  // **************** REDUCER ***********************************************************
  public static class Reduce
       extends Reducer<Text,IntWritable,Text,IntWritable> {
	  
	public class Entry implements Comparable<Entry> {
		    private int keyAsCount;
		    private String valueAsIP;
		    
		    public int getCount() {
		    	return keyAsCount;
		    }
		    
		    public String getIP() {
		    	return valueAsIP;
		    }
	
		    public Entry(int key, String value) {
		        this.keyAsCount = key;
		        this.valueAsIP = value;
		    }		    
	
	    @Override
	    public int compareTo(Entry other) {
	        return this.keyAsCount - other.keyAsCount;
	    }
	}
	
	public static PriorityQueue<Entry> minHeap = new PriorityQueue<Entry>();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
    	
    	int k = context.getConfiguration().getInt("k",1);
        int sum = 0; // initialize the sum for each keyword

        try {
            for (IntWritable val : values) {
        		sum += val.get();  
            }
            
        	if (minHeap.size() >= k) {
        		if (minHeap.peek().getCount() < sum) {
            		minHeap.poll();
            		minHeap.add(new Entry(sum, key.toString()));        			
        		} else {
        			// do nothing..
        		}
        	} else {
        		minHeap.add(new Entry(sum, key.toString()));        		
        	}
        } catch(Exception e) {
            System.out.println("REDUCER: "+e.toString());    	  
        } finally {
			// do nothing..
        }
    }
    
    protected void cleanup (Context context) throws  IOException,InterruptedException
    {
    	while (!minHeap.isEmpty()) {
    		Entry e = minHeap.poll();
			context.write(new Text(e.getIP()), new IntWritable(e.getCount())); // create a pair <keyword, number of occurrences>
    	}
    	minHeap.clear(); // because heap is not required in future!
    }
  }
 
  
  // **************** Driver program ****************************************************
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    
    // source: https://hadoop.apache.org/docs/r1.0.4/api/org/apache/hadoop/util/GenericOptionsParser.html
    String[] options = new GenericOptionsParser(conf, args).getRemainingArgs(); // get all args

    int k = 5; // default value
    if (options.length == 3) {
    	k = Integer.parseInt(options[2]);
    } else if ((options.length < 2) || (options.length > 3)) {
        System.err.println("Usage: LogFileAnalyzer <in> <out> <(OPTIONAL) k>");
        System.exit(-1);    	
    }
    conf.setInt("k", k); // if not used, default value is 1
 

    // create a job with name "LogFileAnalyzer"
    Job job = new Job(conf, "LogFileAnalyzer");
    job.setJarByClass(LogFileAnalyzer.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
   
    // We may add a combiner here, although not required to successfully run the LogFileAnalyzer program  

    // set output key type   
    job.setOutputKeyClass(Text.class);

    // set output value type
    job.setOutputValueClass(IntWritable.class);
    
    //set the HDFS path of the input data
    FileInputFormat.addInputPath(job, new Path(options[0]));

    // set the HDFS path for the output
    FileOutputFormat.setOutputPath(job, new Path(options[1]));

    //Wait till job completion
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
