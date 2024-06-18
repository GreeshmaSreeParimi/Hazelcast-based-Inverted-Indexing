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
import java.util.Hashtable;

import com.hazelcast.core.HazelcastInstanceAware;
import java.util.concurrent.*;
import java.util.Date;
import java.io.Serializable;

/**
 * This class is responsible for current system
 * to join in hazelcast environment,
 * then it gets all the files and count the 
 * Number of instances of keyword  in the file 
 * @param args[0] keyword  to count to count its instances 
 * in all the files.
 */
public class InvertedIndexingLocal {
    public static void main( String[] args ) {
        // validate arguments
        if ( args.length != 1 ) {
            System.out.println( "usage: java InvertedIndexingLocal keyword");
            return;
        }
        // keyword to search in hazelcast files
        String keyword = args[0];

        // prepare a local map
        Hashtable<String, Integer> localMap = 
            new Hashtable<String, Integer>( );
    
        // start hazelcast and retrieve a cached map
        HazelcastInstance hz = Hazelcast.newHazelcastInstance( );
    
        // start a timer
        Date startTimer = new Date( );
        
        IMap<String, String> map = hz.getMap( "files" );
        // examine each file 
        Iterator<String> iter_hazel = map.keySet( ).iterator( );
    
        while ( iter_hazel.hasNext( ) ) {
            String fileName = iter_hazel.next( );
            String value = map.get( fileName );
            String[] words = value.split( " " );
            int wordCounter = 0;

            // prepare a word counter.
            // for each words[i], did you find keyword?
            // if so increment the word counter.
            for(int i=0;i<words.length;i++){
                if(keyword.equals(words[i])){
                    wordCounter++;
                }
            }
            

            /**
             * if the counter is positive
             * put this file name with the counter 
             * value in local hashmap. */ 
            if(wordCounter > 0){
                localMap.put(fileName,wordCounter);
            }
        }
        // before showing the result, stop the timer.
        Date endTimer = new Date( ); 
        
        // Total TCP word count
        int totalCount = 0;
    
        // show the result  
        Iterator<String> iter_local = localMap.keySet( ).iterator( );
        while ( iter_local.hasNext( ) ) {
            String name = iter_local.next( );
            System.out.println( "File[" + name + "] has " 
                + localMap.get( name ) );
            totalCount += localMap.get( name );
        }
        // Print elapsed time
        System.out.println( " " );
        System.out.println( "Elapsed time for local execution = " + 
            ( endTimer.getTime( ) - startTimer.getTime( ) ) );

        // Print total TCP word count
        System.out.println( " " );
        System.out.println( "Total TCP  word count " + totalCount);

    }
}
