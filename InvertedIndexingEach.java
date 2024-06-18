import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.Predicate;
import java.util.Collection;
import java.util.Set;
import java.util.Iterator;
import java.net.InetAddress;
import com.hazelcast.core.HazelcastInstanceAware;
import java.util.concurrent.*;
import java.util.Date;
import java.util.Hashtable;
import java.io.Serializable;
import com.hazelcast.cluster.Member;

/**
 * This class is reponsible 
 * to execute the search functionality where
 * it retrieves local files and search for keyword send
 * by remote machine and return the output.
 */
public class InvertedIndexingEach implements 
    Callable<String>, HazelcastInstanceAware, Serializable {

    private HazelcastInstance hz;
    private String keyword;

    // recieves keyword to search in files
    public InvertedIndexingEach(String keyword){
        this.keyword = keyword;
    }

    @Override
    public void setHazelcastInstance( HazelcastInstance hz ) {
		this.hz = hz;
    }

    /**
     * It executes the keyword search functionality and 
     * returns the map which contains the keyword 
     * count of each file as a string.
     */
    @Override
    public String call( ) throws Exception {

        // retrieve the current system name from cluster
        Member localMember = hz.getCluster( ).getLocalMember( );
        InetAddress localAddress = 
            InetAddress.getByName(localMember.getAddress().getHost());
        String localSystemName = localAddress.getHostName();

        System.out.println("At worker Node : " + localSystemName);

        System.out.println("In call Method of InvertedIndexingEach");
        
        // prepare a local keyword map
        Hashtable<String, Integer> keywordMap = 
            new Hashtable<String, Integer>( );
        
        // retrieve the files map
		IMap<String, String> filesMap = hz.getMap( "files" );

        System.out.println("Local file count ::" 
            + filesMap.localKeySet().size());
		
        //iterator to iterate the local key set
		Iterator<String> iter = filesMap.localKeySet( ).iterator( );

        /**
         * Iterate through all the files and 
         * search for keyword  and maintain count
         * for each file in a keyword map
         *  */ 
		while ( iter.hasNext( ) ) {
            // gets filename
            String fileName = iter.next( );
            // gets file content
            String value = filesMap.get( fileName );
            //splits file count in to array of words
            String[] words = value.split( " " );
            
            int wordCounter = 0;

            /**
             * for each words[i], 
             * If you find keyword? 
             * if so increment the word counter.
             *  */ 
            for(int i=0;i<words.length;i++){
                if(keyword.equals(words[i])){
                    wordCounter++;
                }
            }
            
            /**
             * if the counter is positive
             * put this file name with the counter 
             * value in local hashtable
             */

            if(wordCounter > 0){
                keywordMap.put(fileName,wordCounter);
            }
		}
        System.out.println( " " );
        System.out.println("Output from Cluster memebr ::: " +
            hz.getCluster( ).getLocalMember( ));
        
        System.out.println( " " );
        System.out.println(keywordMap.toString());
        System.out.println( " " );
        return keywordMap.toString();
    }
    
}
