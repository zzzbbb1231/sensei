package com.senseidb.ba.gazelle.utils;

import java.util.Comparator;

import com.senseidb.ba.gazelle.SecondarySortedForwardIndex.SortedRegion;



public class SortUtil {
  private static final int SMALL = 7;
  private static final int MEDIUM = 40;
  
  
  /** A type-specific {@link Comparator}; provides methods to compare two primitive types both as objects
   * and as primitive types. 
   *
   * <P>Note that <code>fastutil</code> provides a corresponding abstract class that
   * can be used to implement this interface just by specifying the type-specific
   * comparator.
   *
   * @see Comparator
   */
  public static interface IntComparator extends Comparator<Integer> {
   /** Compares the given primitive types.
     *
     * @see java.util.Comparator
     * @return A positive integer, zero, or a negative integer if the first
     * argument is greater than, equal to, or smaller than, respectively, the
     * second one.
     */
   public int compare( int k1, int k2 );
  }
  
  public static interface Swapper {
    /** Swaps the data at the given positions.
     * 
     * @param a the first position to swap.
     * @param b the second position to swap.
     */
    void swap( int a, int b );
  }
  
  /** Sorts the specified range of elements using the specified swapper and according to the order induced by the specified
   * comparator using quicksort. 
   * 
   * <p>The sorting algorithm is a tuned quicksort adapted from Jon L. Bentley and M. Douglas
   * McIlroy, &ldquo;Engineering a Sort Function&rdquo;, <i>Software: Practice and Experience</i>, 23(11), pages
   * 1249&minus;1265, 1993.
   * 
   * @param from the index of the first element (inclusive) to be sorted.
   * @param to the index of the last element (exclusive) to be sorted.
   * @param comp the comparator to determine the order of the generic data.
   * @param swapper an object that knows how to swap the elements at any two positions.
   * 
   */
  public static void quickSort( final int from, final int to, final IntComparator comp, final Swapper swapper ) {
    final int len = to - from;
    // Insertion sort on smallest arrays
    if ( len < SMALL ) {
      for ( int i = from; i < to; i++ )
        for ( int j = i; j > from && ( comp.compare( j - 1, j ) > 0 ); j-- ) {
          swapper.swap( j, j - 1 );
        }
      return;
    }

    // Choose a partition element, v
    int m = from + len / 2; // Small arrays, middle element
    if ( len > SMALL ) {
      int l = from;
      int n = to - 1;
      if ( len > MEDIUM ) { // Big arrays, pseudomedian of 9
        int s = len / 8;
        l = med3( l, l + s, l + 2 * s, comp );
        m = med3( m - s, m, m + s, comp );
        n = med3( n - 2 * s, n - s, n, comp );
      }
      m = med3( l, m, n, comp ); // Mid-size, med of 3
    }
    // int v = x[m];

    int a = from;
    int b = a;
    int c = to - 1;
    // Establish Invariant: v* (<v)* (>v)* v*
    int d = c;
    while ( true ) {
      int comparison;
      while ( b <= c && ( ( comparison = comp.compare( b, m ) ) <= 0 ) ) {
        if ( comparison == 0 ) {
          if ( a == m ) m = b; // moving target; DELTA to JDK !!!
          else if ( b == m ) m = a; // moving target; DELTA to JDK !!!
          swapper.swap( a++, b );
        }
        b++;
      }
      while ( c >= b && ( ( comparison = comp.compare( c, m ) ) >= 0 ) ) {
        if ( comparison == 0 ) {
          if ( c == m ) m = d; // moving target; DELTA to JDK !!!
          else if ( d == m ) m = c; // moving target; DELTA to JDK !!!
          swapper.swap( c, d-- );
        }
        c--;
      }
      if ( b > c ) break;
      if ( b == m ) m = d; // moving target; DELTA to JDK !!!
      else if ( c == m ) m = c; // moving target; DELTA to JDK !!!
      swapper.swap( b++, c-- );
    }

    // Swap partition elements back to middle
    int s;
    int n = to;
    s = Math.min( a - from, b - a );
    vecSwap( swapper, from, b - s, s );
    s = Math.min( d - c, n - d - 1 );
    vecSwap( swapper, b, n - s, s );

    // Recursively sort non-partition-elements
    if ( ( s = b - a ) > 1 ) quickSort( from, from + s, comp, swapper );
    if ( ( s = d - c ) > 1 ) quickSort( n - s, n, comp, swapper );
  }
  /**
   * Swaps x[a .. (a+n-1)] with x[b .. (b+n-1)].
   */
  private static void vecSwap( final Swapper swapper, int from, int l, final int s ) {
    for ( int i = 0; i < s; i++, from++, l++ ) swapper.swap( from, l );
  }
  /**
   * Returns the index of the median of the three indexed chars.
   */
  private static int med3( final int a, final int b, final int c, final IntComparator comp ) {
    int ab = comp.compare( a, b );
    int ac = comp.compare( a, c );
    int bc = comp.compare( b, c );
    return ( ab < 0 ?
        ( bc < 0 ? b : ac < 0 ? c : a ) :
        ( bc > 0 ? b : ac > 0 ? c : a ) );
  }
  
  public static int  binarySearch(SortedRegion[] a, int fromIndex, int toIndex, int key) {
    
    int low = fromIndex;
    int high = toIndex - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      int midVal = a[mid].maxDocId;
      int cmp = midVal - key;

      if (cmp < 0)
        low = mid + 1;
      else if (cmp > 0)
        high = mid - 1;
      else
        return mid; // key found
    }
    return -(low + 1); // key not found.
  }
}
