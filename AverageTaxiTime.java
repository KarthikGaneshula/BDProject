import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class AverageTaxiTime extends Configured implements Tool{
    @Override
    public int run(String[] str) throws Exception {
        try{
            FileSystem fs = FileSystem.get(getConf());
            Job job =  Job.getInstance(getConf());
            job.setJarByClass(getClass());
            FileInputFormat.setInputPaths(job, new Path(str[0]));

            job.setMapperClass(AverageTaxiMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(AverageTaxiReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);

            FileOutputFormat.setOutputPath(job, new Path(str[1]));
            if (fs.exists(new Path(str[1])))
            	fs.delete(new Path(str[1]), true);

            job.waitForCompletion(true);
            return job.waitForCompletion(true) ? 0: 1;
        }catch (Exception ex){
            ex.printStackTrace();
        }
        return 0;
    }
    public static void main(String[] arg) throws Exception{
        System.exit(ToolRunner.run(new AverageTaxiTime(), arg));
    }
}

//A Mapper class to filter airports and there taxi time (both in and out). 

class AverageTaxiMapper extends Mapper<Object, Text, Text, LongWritable>{
    public void map(Object obj, Text value, Context context) throws IOException, InterruptedException
    {
        String[] tokens = value.toString().split(",");
        String dest = tokens[17];
        String taxiInTime = tokens[19];
        String source = tokens[16];
        String taxiOutTime = tokens[20];

        if(!dest.equals("NA") && !dest.equals("Dest") && !taxiInTime.equals("NA"))
            context.write(new Text(dest), new LongWritable(Integer.parseInt(taxiInTime)));

        if(!source.equals("NA") && !source.equals("Origin") && !taxiOutTime.equals("NA"))
            context.write(new Text(source), new LongWritable(Integer.parseInt(taxiOutTime)));
    }
}

//Reducer Class is calculating Average taxi time on an airport and adding top 3 and least 3 airports

class AverageTaxiReducer extends Reducer<Text, LongWritable, Text, DoubleWritable>{
    double max1 = 0, max2 = 0, max3 = 0;
    double min1 = Double.MAX_VALUE, min2 = Double.MAX_VALUE, min3 = Double.MAX_VALUE;
    Text taxiMax1 = new Text();
    Text taxiMax2 = new Text();
    Text taxiMax3 = new Text();
    Text taxiMin1 = new Text();
    Text taxiMin2 = new Text();
    Text taxiMin3 = new Text();

    public void reduce(Text key_1, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
    {
        int count = 0, sum = 0;

        for (LongWritable val : values)
        {
            count = count+1;
            sum += val.get();
        }

        double avg = (double)sum / (double)count;

        if (avg > max1)
            swapMax1(avg, key_1.toString());
        else if (avg > max2)
            swapMax2(avg, key_1.toString());
        else if (avg > max3)
            swapMax3(avg, key_1.toString());

        if (avg < min1)
            swapMin1(avg, key_1.toString());
        else if (avg < min2)
            swapMin2(avg, key_1.toString());
        else if (avg < min3)
            swapMin3(avg, key_1.toString());

    }

    
    public void swapMax1(double avg, String val)
    {
        max3 = max2;
        taxiMax3.set(taxiMax2.toString());
        max2 = max1;
        taxiMax2.set(taxiMax1.toString());
        max1 = avg;
        taxiMax1.set(val.toString());
    }

    public void swapMax2(double avg, String val)
    {
        max3 = max2;
        taxiMax3.set(taxiMax2.toString());
        max2 = avg;
        taxiMax2.set(val.toString());
    }

    public void swapMax3(double avg, String val)
    {
        max3 = avg;
        taxiMax3.set(val.toString());
    }

    public void swapMin1(double avg, String val)
    {
        min3 = min2;
        taxiMin3.set(taxiMin2.toString());
        min2 = min1;
        taxiMin2.set(taxiMin1.toString());
        min1 = avg;
        taxiMin1.set(val.toString());
    }

    public void swapMin2(double avg, String val)
    {
        min3 = min2;
        taxiMin3.set(taxiMin2.toString());
        min2 = avg;
        taxiMin2.set(val.toString());
    }

    public void swapMin3(double avg, String val)
    {
        min3 = avg;
        taxiMin3.set(val.toString());
    }

    //used to check the max and min values and display output
    
    
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    	// for top 3 with highest avg tax timings
        if(max1==0.0 && max2==0.0 && max3==0.0)
            context.write(new Text("There are no Taxi times for given input(s)"), null);
        else{
            context.write(new Text("The following are the three highest average taxi time "), null);
            context.write(taxiMax1,new DoubleWritable(max1));
            if(max2==0.0)
                context.write(new Text("There are no further taxi times"), null);
            else
                context.write(taxiMax2, new DoubleWritable(max2));

            if(min3 == 0.0)
                context.write(new Text("There are no further taxi times."), null);
            else
                context.write(taxiMax3, new DoubleWritable(max3));

        }

        context.write(null, null);
        //for top 3 with lowest avg taxi time
        
        if(min1==Double.MAX_VALUE && min2==Double.MAX_VALUE && min3==Double.MAX_VALUE)
            context.write(new Text("There are no Taxi times for given input(s)."), null);
        else {
            context.write(new Text("The following are the three lowest average taxi time."), null);
            context.write(taxiMin1,new DoubleWritable(min1));
            if(min2==0.0)
                context.write(new Text("There are no further taxi times."), null);
            else
                context.write(taxiMin2, new DoubleWritable(min2));

            if(min3==0.0)
                context.write(new Text("There are no further taxi times."), null);
            else
                context.write(taxiMin3, new DoubleWritable(min3));
        }
    }
}
