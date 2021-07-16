import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FlightScheduleProbability extends Configured implements org.apache.hadoop.util.Tool {
    public int run(String[] str) throws Exception{
        try{
            FileSystem fs = FileSystem.get(getConf());
            Job flightjob = Job.getInstance(getConf());
            flightjob.setJarByClass(getClass());
            org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(flightjob, new Path(str[0]));

            flightjob.setMapperClass(FlightScheduleMapper.class);
            flightjob.setMapOutputKeyClass(Text.class);
            flightjob.setMapOutputValueClass(LongWritable.class);

            flightjob.setReducerClass(FlighScheduleReducer.class);
            flightjob.setOutputKeyClass(Text.class);
            flightjob.setOutputValueClass(LongWritable.class);
            org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(flightjob, new Path(str[1]));

            if (fs.exists(new Path(str[1])))
                fs.delete(new Path(str[1]), true);

            flightjob.waitForCompletion(true);
            return flightjob.waitForCompletion(true) ? 0 : 1;
        }catch (Exception ex){
            ex.printStackTrace();
        }
        return 0;
    }
    public static void main(String[] args) throws Exception{
        System.exit(ToolRunner.run(new FlightScheduleProbability(), args));
    }
    public static class StringComparator extends WritableComparator
    {
        public StringComparator()
        {
            super(Text.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
        {
            try{
                String v1 = Text.decode(b1, s1, l1);
                String v2 = Text.decode(b2, s2, l2);
                return v1.compareTo(v2);
            }
            catch (Exception e)
            {
                throw new IllegalArgumentException(e);
            }
        }
    }
}

//A Mapper class to filter delayed flights

class FlightScheduleMapper extends Mapper<LongWritable, Text, Text, LongWritable>
{
    public void map(LongWritable lineOffSet, Text record, Context context) throws IOException, InterruptedException
    {
        String[] col = record.toString().split(",");

        if (col[0] != null && !col[8].equals("UniqueCarrier"))
        {
            String uniqCarrier = col[8];
            String delay = col[14];

            if (uniqCarrier != null && delay != null && !uniqCarrier.trim().equals("") && !delay.trim().equals(""))
            {
                try
                {
                    Integer arrDelayInt = Integer.parseInt(delay);
                    context.write(new Text("All: " + uniqCarrier), new LongWritable(1));

                    if (arrDelayInt > 10)
                        context.write(new Text("Delayed: " + uniqCarrier), new LongWritable(1));
                }
                catch (Exception e)
                {
                }
            }
        }
    }
}

//Reducer Class is calculating Probablity of delayed flights

 class FlighScheduleReducer<KEY> extends Reducer<Text, LongWritable, Text, DoubleWritable>
{
    Map<String, Long> map = new HashMap<String, Long>();

    double max1 = 0L, max2 = 0L, max3 = 0L;
    double min1 = Double.MAX_VALUE, min2 = Double.MAX_VALUE, min3 = Double.MAX_VALUE;
    Text uniqueCarrierMax1 = new Text();
    Text uniqueCarrierMax2 = new Text();
    Text uniqueCarrierMax3 = new Text();
    Text uniqueCarrierMin1 = new Text();
    Text uniqueCarrierMin2 = new Text();
    Text uniqueCarrierMin3 = new Text();

    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
    {
        try
        {
            if(key.toString().contains("All: "))
            {
                String uniqueCarrier = key.toString().replace("All: ", "");
                long sum = 0;

                for (LongWritable val : values)
                	sum++;

                map.put(uniqueCarrier, sum);
            }
            else if(key.toString().contains("Delayed: "))
            {
                long sum = 0;
                String uniqueCarrier = key.toString().replace("Delayed: ", "");
                long total = map.get(uniqueCarrier);

                for (LongWritable val : values)
                    sum++;

                double dprobability = sum / (double) total;
                double prob = 1 - dprobability;
                
                if (prob > max1)
                    swapMax1(prob, uniqueCarrier);
                else if (prob > max2)
                    swapMax2(prob, uniqueCarrier);
                else if (prob > max3)
                    swapMax3(prob, uniqueCarrier);

                if (prob < min1)
                    swapMin1(prob, uniqueCarrier);
                else if (prob < min2)
                    swapMin2(prob, uniqueCarrier);
                else if (prob < min3)
                    swapMin3(prob, uniqueCarrier);
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public void swapMax1(double val, String carrier)
    {
        max3 = max2;
        uniqueCarrierMax3.set(uniqueCarrierMax2.toString());
        max2 = max1;
        uniqueCarrierMax2.set(uniqueCarrierMax1.toString());
        max1 = val;
        uniqueCarrierMax1.set(carrier);
    }

    public void swapMax2(double val, String carrier)
    {
        max3 = max2;
        uniqueCarrierMax3.set(uniqueCarrierMax2.toString());
        max2 = val;
        uniqueCarrierMax2.set(carrier);
    }

    public void swapMax3(double val, String carrier)
    {
        max3 = val;
        uniqueCarrierMax3.set(carrier);
    }

    public void swapMin1(double val, String carrier)
    {
        min3 = min2;
        uniqueCarrierMin3.set(uniqueCarrierMin2.toString());
        min2 = min1;
        uniqueCarrierMin2.set(uniqueCarrierMin1.toString());
        min1 = val;
        uniqueCarrierMin1.set(carrier);
    }

    public void swapMin2(double val, String carrier)
    {
        min3 = min2;
        uniqueCarrierMin3.set(uniqueCarrierMin2.toString());
        min2 = val;
        uniqueCarrierMin2.set(carrier);
    }

    public void swapMin3(double val, String carrier)
    {
        min3 = val;
        uniqueCarrierMin3.set(carrier);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
        context.write(new Text("Highest probability of flights which are as scheduled"), null);
        context.write(uniqueCarrierMax1, new DoubleWritable(max1));
        context.write(uniqueCarrierMax2, new DoubleWritable(max2));
        context.write(uniqueCarrierMax3, new DoubleWritable(max3));
        context.write(null, null);
        context.write(new Text("Lowest probability of flights which are as scheduled"), null);
        context.write(uniqueCarrierMin1, new DoubleWritable(min1));
        context.write(uniqueCarrierMin2, new DoubleWritable(min2));
        context.write(uniqueCarrierMin3, new DoubleWritable(min3));
    }
}