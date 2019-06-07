// https://github.com/manuparra/MasterDatCom_BDCC_Practice/blob/master/starting_hadoop.md

import java.io.IOException;

import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.FileOutputFormat;


// Error on using different classes, so only one will be used with all the rest
public class DevSTD
{
  public static class DevSTDMapper extends MapReduceBase
                                   implements Mapper<LongWritable,
                                                     Text,
                                                     Text,
                                                     DoubleWritable> {


    public void map(LongWritable key,
                    Text value,
                    OutputCollector<Text, DoubleWritable> output,
                    Reporter reporter)
                    throws IOException {

      DoubleWritable aux_double;
      Text aux_text;
      String [] tokens = value.toString().split(",");

      if (!tokens[0].contains("f1"))
      {
        int COLUMN_MAX = 9;
        for (int i = 0; i < COLUMN_MAX; i++)
        {
          aux_text = new Text(Integer.toString(i));
          aux_double = new DoubleWritable(Double.parseDouble(tokens[i]));
          output.collect(aux_text, aux_double);
        }
      }
    }
  }

  public static class DevSTDReducer extends MapReduceBase
                                    implements Reducer<Text,
                                                       DoubleWritable,
                                                       Text,
                                                       DoubleWritable> {

    private ArrayList<Double> arraylist;

    public void reduce(Text key,
                       Iterator<DoubleWritable> values,
                       OutputCollector<Text, DoubleWritable> output,
                       Reporter reporter) throws IOException {

      arraylist = new ArrayList<>();

      double mean = 0.0;
      double aux_double;
      while (values.hasNext())
      {
        aux_double = values.next().get();
        arraylist.add( aux_double );
        mean += aux_double;
      }

      mean = mean/arraylist.size();

      double summatory = 0.0;

      for(double num: arraylist)
      {
        summatory += Math.pow(num - mean, 2);
      }

      double std_deviation = Math.sqrt(summatory / (arraylist.size() - 1));

      output.collect(key, new DoubleWritable(std_deviation));
    }
  }

  public static void main(String[] args) throws Exception
  {
    if (args.length == 2)
    {
      JobConf job = new JobConf(DevSTD.class);
      job.setJobName("DevSTD");

      String in_filepath = args[0];
      String out_filepath = args[1];
      FileInputFormat.addInputPath(job, new Path(in_filepath));
      FileOutputFormat.setOutputPath(job, new Path(out_filepath));

      job.setMapperClass(DevSTDMapper.class);
      job.setReducerClass(DevSTDReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(DoubleWritable.class);
      JobClient.runJob(job);
    }
    else
    {
      System.exit(1);
    }
  }
}
