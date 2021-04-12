package org.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.*;
import java.util.*;

public class PageRank {
    // the opportunity;
    final static double D = 0.85; // damping factor,
    final static int N = 875713;  // Nodes amount
    static int iterRound = 0;
    static boolean autoIteration = true;

    // java PageRank Input output [interating] -- optional arguement
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // get the node size from the website;

        // file path and the time that needs to iterate.
        String dataSource = null;
        int iterTimes = 40;

        if(args.length < 2){
            System.out.println("invalid argument.");
            System.exit( 1 );
        }
        if(args.length>2){
            try{
                iterTimes = Integer.parseInt( args[2] );
            }
            catch (Exception e){
                System.err.println("it is not an integer.");
            }
        }

//        JobControl jobCtrl = new JobControl("PageRankChain");
        // The configuration of the first step
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf,"PageNodes");
//        Job job2 = Job.getInstance(conf2, "calPageRank");

        job1.setJarByClass( PageRank.class );
        job1.setMapperClass( SiteNodeMapper.class );
        job1.setReducerClass( SiteNodeReducer.class );

        job1.setMapOutputKeyClass( IntWritable.class);
        job1.setMapOutputValueClass( IntWritable.class );

        job1.setOutputKeyClass( IntWritable.class );
        job1.setOutputValueClass( Text.class );

        FileInputFormat.setInputPaths(job1,new Path( args[0] ));
        FileOutputFormat.setOutputPath(job1, new Path( args[1] + "/tmp" ) );

        boolean done =  job1.waitForCompletion(true);

        if(done){
            /**
             * calculate the PageRank
             * */
            // open the file made by hadoop
            File desList = new File( args[1]+"/tmp/part-r-00000" );
            BufferedReader br = new BufferedReader( new FileReader( desList ) );
            String oneLine;

            SortedMap<Integer, SortedMap<Integer,Double>> SMatrix = new TreeMap<>();
            ;
            SortedMap<Integer,Double> R = new TreeMap<>();

            // read the file made by hadoop
            while((oneLine = br.readLine()) != null){
                // oneLine should be fromID to_id1 to_id2 to_id3 ...
                String[] from_tos = oneLine.split( "\\s+" );
                // this is a from_ID to give the out_links
                int fromID = Integer.parseInt( from_tos[0] );


                for(int i = 1; i < from_tos.length; i++){
                    // get the to_IDs.
                    int toID = Integer.parseInt( from_tos[i] );
                    // Need to create a new row by to_id
                    if(SMatrix.get(toID) == null) {
                        // to store fromNodeId and PR value donated by it.
                        SortedMap<Integer,Double> col_value = new TreeMap<>();
                        col_value.put( fromID, (double) 1 / (from_tos.length-1) );
                        // add a col_value pair to a row_map pair, to build a triple.
                        SMatrix.put( toID, col_value);
                    } else{
                        // use toID to access the map stored in that.
                        SortedMap tmp =  SMatrix.get( toID );
                        tmp.put(fromID, (double) 1 / (from_tos.length-1) );
                    }
                }

                // if from_id is not appeared in the to_id.
                if(SMatrix.get( fromID ) == null)
                    SMatrix.put( fromID, new TreeMap<>() );
            }

            System.out.println("Log:: finished reading file: " + SMatrix.size());
            System.out.println("The amount of the Nodes provided is correct? " + (SMatrix.size() == N));

            Iterator<Integer> from_ID = SMatrix.keySet().iterator();
            // initialize the pr value
            double pr = (double) 1 / SMatrix.size();
            System.out.println("initial PR value: " + pr);
            System.out.println("A factor((1-D)/N): " + (1-D)/SMatrix.size());

            while(from_ID.hasNext()){
                int gotoId = from_ID.next();
                R.put( gotoId , pr );
            }
            if(autoIteration){
                while(true){
                    SortedMap tmp = calculatePR( SMatrix, R );
                    if(ifStable( tmp,R,0.0002 )){
                        break;
                    }
                    R = tmp;
                }
            }
            else{
                for(int i = 0 ; i < iterTimes; i++){
                    System.out.println("iteration " + i);
                    R = calculatePR( SMatrix, R );
                }
            }

            // sort the result according to the descent order of the value.
            List<Map.Entry<Integer,Double>> list = new ArrayList<>(R.entrySet());

            list.sort( new Comparator<Map.Entry<Integer, Double>>() {
                @Override
                public int compare(Map.Entry<Integer, Double> o1, Map.Entry<Integer, Double> o2) {
                    return 0 - o1.getValue().compareTo( o2.getValue() );
                }
            } );

            writeFilse( args[1]+"/result.txt", list );
        }else {
            System.exit( 1 );
        }
    }
    /**
     * First step
     * This part is for the get the initial matrix fo the sites.
     *
     * */
    private static class SiteNodeMapper
        extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            // get the src and des nodes
            if(value.toString().charAt( 0 ) == '#')
                return;
            else{
                String[] src_des = value.toString().split( "\\s+" );
                int src = Integer.parseInt( src_des[0] ) ;
                int des = Integer.parseInt( src_des[1] ) ;

                context.write( new IntWritable(src),new IntWritable(des) );
            }
        }
    }
    private static class SiteNodeReducer
        extends Reducer<IntWritable, IntWritable, IntWritable, Text>{
        @Override
        protected void reduce(IntWritable from, Iterable<IntWritable> to, Context context)
            throws IOException, InterruptedException {
            String desString = "";
            SortedSet<Integer> sortedSet = new TreeSet( (Comparator<Integer>) (k1, k2) ->{
                if(k1 > k2)
                    return 1;
                else
                    return -1;
            });

            Iterator<IntWritable> iter = to.iterator();
            while (iter.hasNext()){
                sortedSet.add( iter.next().get()) ;
            }

            desString = sortedSet.toString();
            String desStr = desString.replace( ',',' ' );

            desStr = desStr.substring( 1,desStr.length()-1 );
            desStr = desStr.trim();

            context.write(from, new Text(desStr));
        }
    }

    // use Sparse Matrix multiple the R
    private static SortedMap calculatePR(SortedMap<Integer, SortedMap<Integer, Double>> SMatrix, SortedMap<Integer, Double> PRValue){
        SortedMap<Integer, Double> updatedPRV = new TreeMap<>();
        iterRound++;
        // Use row iterator of the matrix, because some of them without any value in it's row.
        // So we don't need to calculate them.
        Iterator<Integer> toIDs = SMatrix.keySet().iterator();

        // traverse the Matrix basing on the toIDs / rows
        while(toIDs.hasNext()){
            // reset the PR value in each row.
            double onePR = 0;
            int toID = toIDs.next();
            // get the fromIDs/columns in each row.
            Iterator<Integer> fromIDs = SMatrix.get( toID ).keySet().iterator();
            // if there is at least one fromId.
            while(fromIDs.hasNext()){
                // locate a position with a value.
                int fromID = fromIDs.next();
                //  Matrix * R
                // there could be a row with out any value.
                if(SMatrix.get( toID ).size() == 0){
                    break;
                }
                double valInM = SMatrix.get( toID ).get( fromID );
                double valInR = PRValue.get( fromID );
                // use D to avoid spider trap.
                onePR += D * valInM * valInR ;
            }
            // all the row has a minimal val after the calculation to avoid dead-end
            onePR += (1-D)/SMatrix.size();
            // use a new map to store the result.
            updatedPRV.put( toID, onePR );
        }

        // if the result is stable, return the value, or keep calculating.
        if(ifStable( PRValue, updatedPRV,0.000001 )){
            return updatedPRV;
        }else{
            System.out.println("finished calculation after " + iterRound +"times of iteration!");
            return calculatePR( SMatrix, updatedPRV );
        }
    }

    private static boolean ifStable(Map<Integer,Double> PRValue1, Map<Integer,Double> PRValue2 , double factor){
        Iterator<Integer> key = PRValue1.keySet().iterator();
        double distance = 0;

        while(key.hasNext() ){
            int k = key.next();
            distance += Math.pow( PRValue2.get( k ) -  PRValue1.get( k ),2);
        }
        distance = Math.sqrt( distance );
        System.out.println(iterRound+"th iteration distance: " + distance);
        return distance < factor;
    }

    private static void writeFilse(String path, List<Map.Entry<Integer,Double>> list) throws IOException {
        System.out.println(list.size());
        File output = new File( path );
        if(!output.exists()) output.createNewFile();
        BufferedWriter bw = new BufferedWriter( new FileWriter( output,false ) );
        System.out.println("The answer is ");
        for (int i = 0; i < list.size(); i++) {
            if(i < 9)
                System.out.print(list.get(i).getKey() + ": " + list.get(i).getValue() + ", ");
            if(i == 9)
                System.out.println(list.get(i).getKey() + ": " + list.get(i).getValue());

            bw.write(list.get(i).getKey() + ": " + list.get(i).getValue()+"\n");
        }
        bw.close();
    }
}
