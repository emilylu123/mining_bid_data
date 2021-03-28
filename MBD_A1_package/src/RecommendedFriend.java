
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

public class RecommendedFriend {
    private static class MaybeFriend implements Writable {
        private long potentialFriend = -1;
        private long mutualFriend = -1;

        MaybeFriend(){ }

        public MaybeFriend(long potentialFriend, long mutualFriend) {
            this.potentialFriend = potentialFriend;
            this.mutualFriend = mutualFriend;
        }

        public long getPotentialFriend() {
            return potentialFriend;
        }

        public long getMutualFriend() {
            return mutualFriend;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeLong( this.potentialFriend );
            dataOutput.writeLong( this.mutualFriend );
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.potentialFriend = dataInput.readLong();
            this.mutualFriend = dataInput.readLong();
        }

        @Override
        public String toString() {
            return  this.potentialFriend + ",";
        }

        public String toString(boolean equal){
            if(equal)
                return  this.potentialFriend + "( " + this.mutualFriend +"),";
            else
                return  this.potentialFriend + ",";
        }
    }

    /*
     * A has B C D E F G friend, which means B C D E F G has a common friend A.
     * INPUT: offset, a line;
     * OUTPUT: one of A's friend X, One of A' friend Y and A ;
     * So X and Y have a Mutual friend A;
     * */
    private static class FriendRecommendSystemMapper
        extends Mapper<LongWritable, Text, LongWritable, MaybeFriend>{

        private LongWritable aPerson = new LongWritable(); // a person who may have a friend,
        private MaybeFriend recommend= new MaybeFriend();

        @Override
        protected void map(LongWritable key, Text text, Context context)
            throws IOException, InterruptedException {

            String line = text.toString();
            String[] field = line.split( "\t" );

            // if No friend.
            if(field.length < 2) {
                return;
            }
            aPerson = new LongWritable(Long.parseLong( field[0] ));
            String[] friend = field[1].split("," );

            // if his friend only has one friend.
            if(friend.length < 2)
                return;
            // build friendship that has existed
            for( String hisFriend: friend){
                // should not recommend this friend.
                context.write( aPerson,new MaybeFriend(Long.parseLong( hisFriend ), -1) );
            }

            for (int i = 0 ; i < friend.length - 1; i++){
                for(int j = i + 1; j <friend.length; j++ ){
                    // two persons can be recommended to each other by The mutual friend.
                    context.write(new LongWritable(Long.parseLong( friend[i] )), new MaybeFriend(Long.parseLong( friend[j] ), Long.parseLong( field[0] )) );
                    context.write(new LongWritable(Long.parseLong( friend[j] )), new MaybeFriend(Long.parseLong( friend[i] ), Long.parseLong( field[0] )) );
                }
            }
        }
    }

    // Reducer
    // Input: a Person id, and a collection of the combination of a recommended friend and mutual friend.
    // Output: This person id, and the text of the recommended friend content.
    private static class FriendRecommendSystemReducer
        extends Reducer<LongWritable, MaybeFriend, LongWritable, Text>{

        Text recommendList ;
        @Override
        protected void reduce(LongWritable aPerson, Iterable<MaybeFriend> recommends, Context context)
            throws IOException, InterruptedException {

            // Use a map to store recommended friend and mutual friend pairs for aPerson.
            Map<Long, LinkedList> recSet = new LinkedHashMap();

            // traverse the recommends.
            for (MaybeFriend recommendedFriend : recommends){
                // This recommended friend did not appear before.
                if (!recSet.containsKey( recommendedFriend.getPotentialFriend() )){
                    // They are not friend.
                    if(recommendedFriend.getMutualFriend() != -1){
                        // Use a linkedList to store the mutual friends for recommended friend.
                        LinkedList<Long> mutual = new LinkedList<>();
                        // add it in the list.
                        mutual.add( recommendedFriend.getMutualFriend() );
                        recSet.put(recommendedFriend.getPotentialFriend(),mutual);
                    }
                    else{
                        // they are friend, go to the next recommendedFriend.
                        recSet.put(recommendedFriend.getPotentialFriend(),null);
                    }
                }
                // A recommended friend was appeared.
                else {
                    // This recommendedFriend is not friend by now.
                    if(recSet.get( recommendedFriend.getPotentialFriend()) != null && recommendedFriend.getMutualFriend() != -1){
                        // put this new mutual friend to the list.
                        recSet.get( recommendedFriend.getPotentialFriend()).add( recommendedFriend.getMutualFriend() );
                    }
                    else
                        // find they are friend now.
                        recSet.put(recommendedFriend.getPotentialFriend(),null);
                }
            }

            // sort the recommended friends by mutual friend number(The size of the List)
            // if the Size is same use id to sort.
            SortedMap<Long, List> sortedRecSet = new TreeMap( (Comparator<Long>) (k1, k2) -> {
                int f1 = recSet.get( k1 ).size();
                int f2 = recSet.get( k2 ).size();
                if (f1 > f2)
                    return 1;
                else if (f1 == f2 && k1 > k2)
                    return 1;
                else
                    return -1;
            } );

            for(Long key : recSet.keySet()){
                if(recSet.get( key ) != null)
                    sortedRecSet.put(key, recSet.get( key ));
            }

            String text = "";
            int tmpSize = 0;
            int count=0;

            // Use Entry to traverse the map and print the output basing on the requirement.
            for(Map.Entry<Long,List> content: sortedRecSet.entrySet()){
                count++;

                if(tmpSize == content.getValue().size()) {
                    text += content.getKey() + "(" + content.getValue().toString() + "),";
                }
                else {
                    text += content.getKey() + ",";
                    tmpSize = content.getValue().size();
                }
                if(count == 10)
                    break;
            }
            recommendList = new Text(text);
            context.write( aPerson , recommendList);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass( RecommendedFriend.class );

        job.setMapperClass( FriendRecommendSystemMapper.class );
        job.setReducerClass( FriendRecommendSystemReducer.class );

        job.setMapOutputKeyClass( LongWritable.class );
        job.setMapOutputValueClass( MaybeFriend.class );

        job.setOutputKeyClass( LongWritable.class );
        job.setOutputValueClass( Text.class );

        FileInputFormat.setInputPaths( job, new Path( args[0] ) );
        FileOutputFormat.setOutputPath( job, new Path( args[1] ) );

        boolean done = job.waitForCompletion( true );
        System.exit( done? 0 : 1 );
    }
}
