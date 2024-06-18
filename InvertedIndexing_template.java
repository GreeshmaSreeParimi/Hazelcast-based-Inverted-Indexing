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

public class InvertedIndexing_template {
    public static void main( String[] args ) {
	// validate arguments
        if ( args.length != 1 ) {
            System.out.println( "usage: java InvertedIndexingLocal keyword " );
            return;
        }
        String keyword = args[0];

	// prepare a local map
	Hashtable<String, Integer> local = new Hashtable<String, Integer>( );

	// start hazelcast and retrieve a cached map
        HazelcastInstance hz = Hazelcast.newHazelcastInstance( );

	// start a timer
	Date startTimer = new Date( );
	
        IMap<String, String> map = hz.getMap( "files" );
	// examine each file 
        Iterator<String> iter_hazel = map.keySet( ).iterator( );

        while ( iter_hazel.hasNext( ) ) {
            String name = iter_hazel.next( );
			String value = map.get( name );
			String[] words = value.split( " " );
			// prepare a word counter.
			// for each words[i], did you find keyword? if so increment the word counter.

			// if the counter is positive
			// put this file name with the counter value in local hashtable.
        }
	Date endTimer = new Date( ); // before showing the result, stop the timer.

	// show the result  
	Iterator<String> iter_local = local.keySet( ).iterator( );
	while ( iter_local.hasNext( ) ) {
	    String name = iter_local.next( );
	    System.out.println( "File[" + name + "] has " + local.get( name ) );
	}
	System.out.println( " " );
	System.out.println( "Elapsed time = " + ( endTimer.getTime( ) - startTimer.getTime( ) ) );
    }
}
