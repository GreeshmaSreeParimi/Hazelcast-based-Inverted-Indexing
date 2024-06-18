import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MultiExecutionCallback;
import com.hazelcast.config.Config;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.IExecutorService;
import java.util.concurrent.*;
import java.util.*;
import java.net.InetAddress;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.map.IMap;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.Predicate;
import com.hazelcast.core.HazelcastInstanceAware;
import java.io.Serializable;

public class InvertedIndexingHierarchy implements 
    Callable<String>, HazelcastInstanceAware, Serializable {

    private HazelcastInstance hz;
    private String keyword;
    private HashMap<String,HashSet<String>> systemsHierarchyMap = new HashMap<>();

    public InvertedIndexingHierarchy(String keyword, HashMap<String,HashSet<String>> map){
        this.keyword = keyword;
        this.systemsHierarchyMap = map;
    }

    @Override
    public void setHazelcastInstance( HazelcastInstance hz ) {
		this.hz = hz;
    }
    
    @Override
    public String call( ) throws Exception {

        System.out.println("In call Method of InvertedIndexingHierarchy");

        IExecutorService exec = hz.getExecutorService( "exec" );
        
        System.out.println("key word :::: " + this.keyword);
        Callable<String> keyCallable = new InvertedIndexingEach(this.keyword);
        
        // display local system files count
        IMap<String, String> FilesMap = hz.getMap( "files" );
        
        System.out.println("Local File count :::::::::::::" + 
            FilesMap.localKeySet().size());

        /**
         * CountDownLatch is a synchronization mechanism 
         * provided by the java.util.concurrent
         * It makes the program wait
         * set of tasks, represented by a count, 
         * is completed before they can proceed further
         * @param countOfTasks 
         */
        CountDownLatch latch = new CountDownLatch(1);


        final HashMap<String, Integer> keywordCountMap = new HashMap<>();
        
        MultiExecutionCallback callback =
            new MultiExecutionCallback( ) {
    
            @Override
            public void onResponse( Member member, Object msg ) {
               // System.err.println( "Received: " + msg );
            }

            /**
             * It receives the output from only the child 
             * cluster memebers to which it dispatched 
             *  InvertedIndexingEach
             */
            @Override
            public void onComplete( Map<Member, Object> msgs ) {
                try{
                    /**
                     * recieves the data from each child cluster memeber
                     * ang aggregates the data and send its parent memeber
                     * by which it is called i.e In this case 
                     * InvertedIndexingAdditionalFeature 
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
                        if(hashMapAsString.isEmpty()) continue;

                        System.out.println("hashMapAsString ::::" + hashMapAsString);
                        
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
                            keywordCountMap.put(key,Integer.valueOf(value));
                        }
                    }
                    
                }catch(Exception e){
                    System.out.println("In exception");
                }
                // Notify the latch that the task is completed
                latch.countDown(); 
                
            }
        };

        // retrieve all cluster nodes.
        Set<Member> clusterMembers = hz.getCluster( ).getMembers( );

        // retrieve the current system name from cluster
        Member localMember = hz.getCluster( ).getLocalMember( );
        InetAddress localAddress = 
            InetAddress.getByName(localMember.getAddress().getHost());
        String localSystemName = localAddress.getHostName();

        System.out.println("local System Host Name ::::" + localSystemName);

        /**
         * retrieve the child systems for current system 
         * to whom tasks should be deligated
         *  */ 
        HashSet<String> childSystemSet;
        if(systemsHierarchyMap.containsKey(localSystemName)){
            childSystemSet = systemsHierarchyMap.get(localSystemName);
        }else{
            /**
             * return empty string if no child system 
             * are found to delegate
             */
            return keywordCountMap.toString();
        }
        
        
        // iterator to retrieve the members for current system
        Iterator<Member> iterator = clusterMembers.iterator();
        
        // New Set to store child cluster memebrs
        Set<Member> filterdClusterMembers = new HashSet<>();

        /***
         * filtering the members for current system(key) values
         * based on systemsHierarchyMap 
         * This is to dispatch InvertedIndexingEach to second
         * level of systems in hierarchy map
         *  */ 
        while (iterator.hasNext()) {
            Member member = iterator.next();
            InetAddress inetAddress = 
                InetAddress.getByName(member.getAddress().getHost());
            String systemName = inetAddress.getHostName();

            if(childSystemSet.contains(systemName)){
                System.out.println("child System Name  :::: " 
                    + systemName);
                filterdClusterMembers.add(member);
            }
        }

       /**
        * If the filterdClusterMembers == 0 
        * return emptry string
        */
        if(filterdClusterMembers.size() == 0) {
            return keywordCountMap.toString();
        }
            
        // submit cluster memebers
        filterdClusterMembers.add(localMember);
        exec.submitToMembers( keyCallable, filterdClusterMembers, callback );
        
        // Wait for all tasks to complete
        latch.await();
		
        /**
         * After every child cluster memebers return the result
         * return final result.
         */
        return keywordCountMap.toString();
    }
    
}