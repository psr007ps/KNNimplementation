/**
 * 
 */
package com.cs226_asg2_pshri002;

/**
 * @author pranshu shrivastava
 *
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class KNN {
	public static class TokenizerMapper extends Mapper<Object, Text, DoubleWritable, Text>{
        private String queryPoint;
        private DoubleWritable Distance = new DoubleWritable();
        private Text p = new Text();
        private String[] points;
        private Double point1, point2;
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf=context.getConfiguration();
            queryPoint = conf.get("QueryPoint");
            points = queryPoint.split(",");
             /* Get and split query point */
            point1 = Double.parseDouble(points[0]);
            point2 = Double.parseDouble(points[1]);
        }
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            Double dist;
            String str = value.toString();
            String[] splitString = str.split(",");
            double pointSp1 = Double.parseDouble(splitString[1]);
            double pointSp2 = Double.parseDouble(splitString[2]);
            /* Euclidean distance calculation */
            dist = (Math.sqrt(Math.pow((point1-pointSp1),2) + Math.pow((point2-pointSp2),2)));
            //System.out.println(str);
             /* output (distance, point) as key value pair */
            Distance.set(dist);
            p.set(str);
            
            context.write(Distance, p);
        }
    }

    public static class IntSumReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
        private int count = 1;

        protected void setup(Context context) {
            count = 1;
        }

        public void reduce(DoubleWritable distance, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int k1 = conf.getInt("k", 1);
            if (count <= k1) {
                for (Text p : value) {
                    context.write(distance, p);
                    count++;
                }
            }
        }
    }
        
    public static void main( String[] args )throws IOException
    {
        /*String arg0 = "points";
        String arg1 = "1,2";
        String arg2 = "5";
        String arg3 = "KNNoutput4";*/
    	Configuration conf = new Configuration();
        conf.set("QueryPoint",args[1]);
        conf.setInt("k", Integer.parseInt(args[2]));
        Job job = Job.getInstance(conf, "KNN");
        job.setJarByClass(KNN.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setNumReduceTasks(1);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        try {
            job.waitForCompletion(true);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }


}
