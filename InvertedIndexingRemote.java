import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MultiExecutionCallback;
import com.hazelcast.config.Config;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.IExecutorService;
import java.util.concurrent.*;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;
import java.net.InetAddress;

/**
 * This class is reponsible to connect to
 * hazelcast enviroment. It the creates a
 * callable object , callback and dispatches them to all 
 * the cluster members. In the callback methods it aggregates 
 * the output afrom members and prints output.
 * @param args[0] keyword to search in the files content 
 */
public class InvertedIndexingRemote {
    public static void main( String[] args ) throws Exception {

        // validate arguments
        if ( args.length != 1 ) {
            System.out.println( "usage: java InvertedIndexingLocal keyword");
            return;
        }
        // keyword to search in files
        String keyword = args[0];

        // start hazelcast
        HazelcastInstance hz = Hazelcast.newHazelcastInstance( );

        // create exec object
        IExecutorService exec = hz.getExecutorService( "exec" );

        // create callable object
        Callable<String> keyCallable = new InvertedIndexingEach(keyword);

        // start a timer
        final Date startTimer = new Date( );
        
        MultiExecutionCallback callback =
            new MultiExecutionCallback( ) {
    
            @Override
            public void onResponse( Member member, Object msg ) {
               // System.err.println( "Received: " + msg );
            }

            /** 
             * called when all members complete their respective tasks
             * */ 
            @Override
            public void onComplete( Map<Member, Object> msgs ) {
                try{
                    /** local map to aggregate data from cluster members
                     * */ 
                    HashMap<String, Integer> keyWordMap = new HashMap<>();

                    /**
                     * msgs include output from all cluster members
                     */
                    for ( Object msg : msgs.values( ) ) {
                        // parse to string
                        String resultString = (String)msg;
                        if(resultString.length() == 0) continue;

                        /**
                         * remove extra braces at start and 
                         * end of string to convert to hashmap
                         *  */ 
                        String hashMapAsString  = 
                            resultString.substring(1, resultString.length() - 1);
                        
                        // if string is empty , continue.
                        if(hashMapAsString.isEmpty()) continue;

                        System.out.println("hashMapAsString ::::" 
                            + hashMapAsString);
                        
                        /**
                         * split the string to key value pairs
                         *  */
                        String[] keyValuePairs = hashMapAsString.split(",");
                        
                        /**
                         * store each key value pair in local map
                         */
                        for(int i=0;i<keyValuePairs.length;i++){
                            String[] keyValue = keyValuePairs[i].split("=");
                            String key = keyValue[0];
                            String value = keyValue[1];
                            keyWordMap.put(key,Integer.valueOf(value));
                        }
                    }
                    
                    int totalCount = 0;
                    // print keyword count for each file
                    for(Map.Entry<String,Integer> m : keyWordMap.entrySet()){
                        totalCount += m.getValue();
                        System.out.println("File[" + m.getKey() + "] has " 
                            + m.getValue());
                    }
                    final Date endTimer = new Date();

                    // print elapsed time
                    System.out.println( " " );
                    System.out.println( "Elapsed time for Remote exceution = "
                     + ( endTimer.getTime( ) - startTimer.getTime( ) ) );
                    
                    //print total macthed TCP word count
                    System.out.println( " " );
                    System.out.println( "Total TCP  word count "
                     + totalCount);

                }catch(Exception e){
                    System.out.println("In exception");
                }
                
            }
        };
        
        // retrieve all cluster nodes.
        Set<Member> clusterMembers = hz.getCluster( ).getMembers( );
        
        // call InvertedIndexingEach.class at each cluster node
        exec.submitToMembers( keyCallable, clusterMembers, callback );
    }
}
