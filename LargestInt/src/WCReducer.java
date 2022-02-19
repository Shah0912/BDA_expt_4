//import java.io.IOException;
//import java.util.Iterator;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.mapred.MapReduceBase;
//import org.apache.hadoop.mapred.OutputCollector;
//import org.apache.hadoop.mapred.Reducer;
//import org.apache.hadoop.mapred.Reporter;

//public class WCReducer extends MapReduceBase implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
//  int max1 = -1000000000;
//  
//  public void reduce(IntWritable key, Iterator<IntWritable> value, OutputCollector<IntWritable, IntWritable> output, Reporter rep) throws IOException {
//    this.max1 = Math.max(this.max1, key.get());
//    output.collect(key, new IntWritable(this.max1));
//  }
//}

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter; 
  
public class WCReducer extends MapReduceBase implements Reducer<IntWritable, 
                                   IntWritable, Text, FloatWritable> { 
  
    @Override
    // Reduce Function 
    public void reduce(IntWritable key, Iterator<IntWritable> value, 
     OutputCollector<Text, FloatWritable> output, Reporter rep) 
  
    throws IOException 
    { 
    	int max1 = -1000000000;
  
    
//    	int sum = 0; 
//    	int count=0;
        // Counting the frequency of each words 
        while (value.hasNext())  
        { 
            IntWritable i = value.next(); 
            max1 = Math.max(max1, i.get());
//            sum += i.get();
//            count++;
        } 
        output.collect(new Text("Largest = "), new FloatWritable(max1));
//        float sum1= (float)(sum)/count;
//        output.collect(new Text("AVG"), new FloatWritable(sum1)); 
      
    } 
}