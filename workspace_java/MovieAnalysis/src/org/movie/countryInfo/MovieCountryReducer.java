package org.movie.countryInfo;
// cc MaxTemperatureReducer Reducer for maximum temperature example
// vv MaxTemperatureReducer
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.gson.Gson;

public class MovieCountryReducer
  extends Reducer<IntWritable, Text, IntWritable, Text> {
    
    public static boolean ASC = true;
    public static boolean DESC = false;
  
  @Override
  public void reduce(IntWritable key, Iterable<Text> values,
      Context context)
      throws IOException, InterruptedException {
    
     Map<String, Double> temp= new HashMap<String, Double>();
    for (Text value : values) {
        
        String countryName = value.toString();
        if(temp.containsKey(countryName)){
            double count= temp.get(countryName)+1;
            temp.remove(countryName);
            temp.put(countryName, count);
        }else {
            temp.put(countryName, 1.0);
        }     
    }
    Map<String, Double> sortedMap = sortByComparator(temp, DESC);
    Map<String, Double> ratioMap = getRatio(sortedMap);
    Gson gson = new Gson();
    String result = gson.toJson(ratioMap);
    
    context.write(key, new Text(result));
  }
  
  private static Map<String, Double> sortByComparator(Map<String, Double> unsortMap, final boolean order)
  {

      List<Entry<String, Double>> list = new LinkedList<Entry<String, Double>>(unsortMap.entrySet());

      // Sorting the list based on values
      Collections.sort(list, new Comparator<Entry<String, Double>>()
      {
          public int compare(Entry<String, Double> o1,
                  Entry<String, Double> o2)
          {
              if (order)
              {
                  return o1.getValue().compareTo(o2.getValue());
              }
              else
              {
                  return o2.getValue().compareTo(o1.getValue());

              }
          }
      });

      // Maintaining insertion order with the help of LinkedList
      Map<String, Double> sortedMap = new LinkedHashMap<String, Double>();
      for (Entry<String, Double> entry : list)
      {
          sortedMap.put(entry.getKey(), entry.getValue());
      }

      return sortedMap;
  }
  
  private Map<String, Double> getRatio(Map<String, Double> numberMap){
      double sum=0;
      Map<String, Double> ratioMap = new HashMap<String, Double>();
      NumberFormat doubleFormat = new DecimalFormat(".0000");
      
      for(String key:numberMap.keySet()){
          sum+=numberMap.get(key);       
      }
      for(String key:numberMap.keySet()){
          double numberOfEachMovie= numberMap.get(key);
          double ratio=numberOfEachMovie/sum;
          ratio = Double.parseDouble(doubleFormat.format(ratio));
          //numberMap.put(key,numberMap.ge);
          ratioMap.put(key, ratio);        
      }
      
      ratioMap = sortByComparator(ratioMap, DESC);
      
      return ratioMap;
      
  }

}
// ^^ MaxTemperatureReducer
