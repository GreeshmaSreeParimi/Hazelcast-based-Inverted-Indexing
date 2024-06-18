import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MultiExecutionCallback;
import com.hazelcast.config.Config;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.IExecutorService;
import java.util.concurrent.*;
import java.util.*;
import java.net.InetAddress;

/**
 * This file is reposnsible for dispatching the 
 * InvertedIndexingHierarchy to cluster members
 * where selected cluster members again relies 
 * the InvertedIndexingEach to particular cluster members.
 */
public class InvertedIndexingAdditionalFeature {
    
    public static void main( String[] args ) throws Exception {

        // validate arguments
        if ( args.length != 1 ) {
            System.out.println( "usage: java InvertedIndexingLocal keyword");
            return;
        }
        // keyword for searching
        String keyword = args[0];

        HashMap<String,HashSet<String>> systemsHierarchyMap = new HashMap<>();

        systemsHierarchyMap.put("cssmpi2h.uwb.edu",
            new HashSet<>(Set.of("cssmpi4h.uwb.edu", "cssmpi5h.uwb.edu")));
        systemsHierarchyMap.put("cssmpi3h.uwb.edu",
            new HashSet<>(Set.of("cssmpi6h.uwb.edu","cssmpi7h.uwb.edu")));

        // create hazelcast instance
        HazelcastInstance hz = Hazelcast.newHazelcastInstance( );

        // create exec object
        IExecutorService exec = hz.getExecutorService( "exec" );

        /**
         * create InvertedIndexingHierarchy object to 
         * dispatch to other cluster members
         */
        Callable<String> keyCallable = 
            new InvertedIndexingHierarchy(keyword, systemsHierarchyMap);

        // start a timer
        final Date startTimer = new Date( );

       /**
        * callback to be called when cluster memebers 
        * complete their respective job */ 
        MultiExecutionCallback callback =
            new MultiExecutionCallback( ) {
    
            @Override
            public void onResponse( Member member, Object msg ) {
               // System.err.println( "Received: " + msg );
            }
            /**
             * called when all cluster memebers are done
             * with their task.
             */
            @Override
            public void onComplete( Map<Member, Object> msgs ) {
                try{

                    HashMap<String, Integer> keyWordMap = new HashMap<>();

                    for ( Object msg : msgs.values( ) ) {
                        // parse it to string
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

                    int totalcount = 0;
                    
                    // print the keyword count for each file
                    for(Map.Entry<String,Integer> m : keyWordMap.entrySet()){
                        totalcount += m.getValue();
                        System.out.println("File[" + m.getKey() + "] has " +
                             m.getValue());
                    }
                    final Date endTimer = new Date();
                    // prints total time take to hierarchial execution
                    System.out.println( " " );
                    System.out.println( "Elapsed time for Hierarchial Execution = " 
                    + ( endTimer.getTime( ) - startTimer.getTime( ) ) );

                    System.out.println( " " );
                    System.out.println( "Total TCP word count " 
                        + totalcount);
                }catch(Exception e){
                    System.out.println("In exception " + e);
                }
                
            }
        };
        
        // retrieve all cluster nodes.
        Set<Member> clusterMembers = hz.getCluster( ).getMembers( );

        // iterator to retrieve the members for current system
        Iterator<Member> iterator = clusterMembers.iterator();

        // New Set to store child cluster memebrs
        Set<Member> filterdClusterMembers = new HashSet<>();

        /***
         * filtering the members based on the "key" of 
         * systemsHierarchyMap
         * This is to dispatch InvertedIndexingHierarchy to first
         * level of systems in hierarchy map
         * */ 
        while (iterator.hasNext()) {
            Member member = iterator.next();
            InetAddress inetAddress = 
                InetAddress.getByName(member.getAddress().getHost());
            String systemName = inetAddress.getHostName();

            if(systemsHierarchyMap.containsKey(systemName)){
                filterdClusterMembers.add(member);
            }
        }
        
        // call InvertedIndexingHierarchy.class at filterdClusterMembers
        exec.submitToMembers( keyCallable, filterdClusterMembers, callback );
    }

}