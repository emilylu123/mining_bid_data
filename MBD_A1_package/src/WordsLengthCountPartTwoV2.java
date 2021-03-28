
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import java.util.HashMap;
import java.util.Map;

public class WordsLengthCountPartTwoV2 extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordsLengthCountPartTwoV2(), args);
        System.exit( res );
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf(), "WordsLengthCountPartTwo");
        job.setJarByClass( WordsLengthCountPartTwoV2.class );

        job.setMapOutputKeyClass( IntWritable.class );
        job.setMapOutputValueClass( Text.class );

        job.setOutputKeyClass( IntWritable.class );
        job.setOutputValueClass( IntWritable.class );

        job.setMapperClass( WordsLengthCountPartTwoMapper.class );
        job.setReducerClass( WordsLengthCountPartTwoReducer.class );

        FileInputFormat.setInputPaths( job, new Path( args[0] ) );
        FileOutputFormat.setOutputPath( job, new Path( args[1] ) );

        boolean done = job.waitForCompletion( true );

        return done ? 0 : 1;
    }

    /*
     * split the lien by any character which is not letter.
     * */
    private static class WordsLengthCountPartTwoMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
        Text out = null;

        @Override
        protected void map(LongWritable key, Text text, Context context)
            throws IOException, InterruptedException {
            String line = text.toString().toLowerCase();
            String [] word = line.split( " " );
            for(int i = 0 ; i < word.length; i++){
                String tmp = word[i].replaceAll( "[^\\w]","" );
                int len = tmp.length();
                out = new Text(tmp);
                context.write( new IntWritable(len),out );
            }
        }
    }

    /*
     * The input key is Integer standing for the length of the word, and value is the iterator of the words.
     * The output key is the Integer standing for the length of the word, and value is the integer standing for the number of the words.
     * */
    private static class WordsLengthCountPartTwoReducer extends Reducer<IntWritable, Text, IntWritable, IntWritable> {

        @Override
        protected void reduce(IntWritable length, Iterable<Text> textSet, Context context) throws IOException, InterruptedException {

            Map<String,Integer> tmpSet = new HashMap<>();
            while(textSet.iterator().hasNext()){
                String word = textSet.iterator().next().toString();
                if(tmpSet.isEmpty())
                    tmpSet.put( word,1 );
                else
                if(!tmpSet.containsKey( word ))
                    tmpSet.put( word,1 );
                else
                    continue;
            }
            IntWritable frequency =new IntWritable(tmpSet.size());
            context.write( length,frequency );
        }
    }
}

