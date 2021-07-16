import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;

public class FlightCancellation extends Configured implements org.apache.hadoop.util.Tool {
    public int run(String[] args) throws Exception{
        try{
            FileSystem filesystem = FileSystem.get(getConf());
            Job job = Job.getInstance(getConf());
            job.setJarByClass(getClass());
            org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(job, new Path(args[0]));

            job.setMapperClass(CancellationMyMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);

            job.setReducerClass(CancellationMyReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, new Path(args[1]));

            if (filesystem.exists(new Path(args[1])))
            	filesystem.delete(new Path(args[1]), true);

            job.waitForCompletion(true);
            return job.waitForCompletion(true) ? 0 : 1;
        }catch (Exception ex){
            ex.printStackTrace();
        }
        return 0;
    }
    public static void main(String[] args) throws Exception{
        System.exit(ToolRunner.run(new FlightCancellation(), args));
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

            try
            {
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

//A Mapper class to get different type of cancellation reason. A = carrier, B = weather, C = NAS, D = security

class CancellationMyMapper extends Mapper<LongWritable, Text, Text, LongWritable>
{
    public void map(LongWritable lineOffSet, Text record, Context context) throws IOException, InterruptedException
    {
        String[] tokens = record.toString().split(",");

        String cancel = tokens[21];
        String code = tokens[22];
        if(!cancel.equals("NA") && !cancel.equals("Cancelled") && cancel.equals("1") &&
                !code.equals("NA") && !code.equals("CancellationCode") && !code.isEmpty()){
            context.write(new Text(code), new LongWritable(1));
        }
    }
}


//Reducer Class is calculating number of flight cancelled due to following reason and then getting the most common reason.

class CancellationMyReducer<KEY> extends Reducer<Text, LongWritable, Text, LongWritable>
{
    long max = 0L;
    Text topCancellation = new Text();

    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;

        for (LongWritable val : values)
            sum += val.get();

        if (sum >= max) {
        	topCancellation.set(key);
            max = sum;
        }
    }
        protected void cleanup(Context context) throws IOException, InterruptedException
        {
        	switch(topCancellation.toString()) {
        	case "A":
        		context.write(new Text("Most Common Reason for Flight Cancellation is: Carrier"), null);
        		break;
        	case "B":
        		context.write(new Text("Most Common Reason for Flight Cancellation is: Weather"), null);
        		break;
        	case "C":
        		context.write(new Text("Most Common Reason for Flight Cancellation is: NAS"), null);
        		break;
        	case "D":
        		 context.write(new Text("Most Common Reason for Flight Cancellation is: Security "), null);
        		 break;
        	default:
        		context.write(new Text("There are no cancellation flights according to the input files."), null);
        	}
        	
        }

}