package org.movie.countryInfo;
// cc MaxTemperatureMapper Mapper for maximum temperature example
// vv MaxTemperatureMapper
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieCountryMapper
  extends Mapper<LongWritable, Text, IntWritable, Text> {

 // private static final int MISSING = 9999;
  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    String line = value.toString();
   
    
    String [] movieInfoStrings = line.split("%%%");
    assert(movieInfoStrings.length==3);
    int year=0;
    try {
        year= Integer.parseInt(movieInfoStrings[1]);
    } catch (Exception e) {
        System.out.println("It's not value!\n");
        for(String tempString:movieInfoStrings){
            System.out.println(tempString+"\n");
        }
        System.out.println(movieInfoStrings.length);
        System.out.println(line);
        }
        
    
    
    context.write(new IntWritable(year), new Text(movieInfoStrings[2]));
    
    

  }
}
// ^^ MaxTemperatureMapper
