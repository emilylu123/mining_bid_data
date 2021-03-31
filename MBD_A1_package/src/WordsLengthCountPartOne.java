
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeMap;

public class WordsLengthCountPartOne {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"WordsLengthCountPartOne");
        job.setJarByClass( WordsLengthCountPartOne.class );

        job.setMapperClass( WordLengthCountMapper.class );
        job.setReducerClass( WordLengthCountReducer.class );

        job.setOutputKeyClass( IntWritable.class );
        job.setOutputValueClass( Text.class );

        FileInputFormat.setInputPaths( job, new Path( args[0] ) );
        FileOutputFormat.setOutputPath( job, new Path( args[1] ) );

        boolean done =  job.waitForCompletion(true);
        System.exit( done?0:1 );

    }
    // Read the file,
    // map the words by the length of the words.
    private static class WordLengthCountMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
        StringTokenizer st = null;
        @Override
        protected void map(LongWritable key, Text text, Context context)
            throws IOException, InterruptedException {
            //  want to use regular expression as the symbol to split different words.

            st = new StringTokenizer( text.toString().toLowerCase() );
            while(st.hasMoreTokens()){
                // only letters are caculated
                String oneWord = st.nextToken();
                int len = 0;
                for(int i =0; i<oneWord.length(); i++){
                    if(Character.isLetter(oneWord.charAt( i ))){
                        len++;
                    }
                }
                context.write( new IntWritable(len), new Text(oneWord) );
            }
        }
    }

    // get the data from map.
    private static class WordLengthCountReducer extends Reducer<IntWritable, Text, IntWritable, Text>{
        @Override
        protected void reduce(IntWritable key, Iterable<Text> textCollection, Context context)
            throws IOException, InterruptedException {

            TreeMap<String, Integer> wordCountMap = new TreeMap<>();

            int wordNumWithSameLength = 0;
            while(textCollection.iterator().hasNext()){
                wordNumWithSameLength++;
                // for logs
                String keyWord = textCollection.iterator().next().toString();
                if(wordCountMap.containsKey( keyWord )){
                    int tmp = wordCountMap.get(keyWord).intValue() + 1;
                    wordCountMap.put( keyWord, tmp );
                }
                else
                    wordCountMap.put(keyWord,1);
            }
            String statistic = wordNumWithSameLength + "\n" ;

            // Print the log of detail that how many times a word appeared.
//            for(Map.Entry<String,Integer> entry : wordCountMap.entrySet()){
//                statistic +=  entry.getKey() + " " + entry.getValue() + "\n";
//            }
            context.write( key,new Text(statistic) );
        }
    }
}

