package org.movie.countryInfo;
// cc MaxTemperature Application to find the maximum temperature in the weather dataset
// vv MaxTemperature
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieAnalysis {

  public static void main(String[] args) throws Exception {
//    if (args.length != 2) {
//      System.err.println("Usage: MaxTemperature <input path> <output path>");
//      System.exit(-1);
//    }
      String input = "/Users/aaronwong/Documents/workspace_java/DataPreprocessing/src/data/out_countries.list";
      String output = input+"_out";
    
    Job job = new Job();
    job.setJarByClass(MovieAnalysis.class);
    job.setJobName("Max temperature");

    FileInputFormat.addInputPath(job, new Path(input));
    FileOutputFormat.setOutputPath(job, new Path(output));
    
    job.setMapperClass(MovieCountryMapper.class);
    job.setReducerClass(MovieCountryReducer.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
// ^^ MaxTemperature
