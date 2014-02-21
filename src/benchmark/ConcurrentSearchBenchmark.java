
import edu.stanford.ppl.concurrent.*;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.SortedMap;

public class ConcurrentSearchBenchmark implements Runnable {

  // Benchmark parameters
  private int warmup_iterations; /* iterations to reach steady-state */
  private int timing_iterations; /* number of timing iterations */
  private float max_cov; /* maximum CoV for usable timing */
  private int num_threads;
  private int key_range;  /* difference between max and min key */
  private int num_operations;
  private float percent_insert;
  private float percent_remove;
  private float percent_search;
  private int map_algorithm; /* identifier for implementation of SortedMap */

  // constants
  private static final int MAX_ITERATIONS = 30;
  private static final int NUM_ARGS = 10;

  // timing
  private static long[] times = new long[MAX_ITERATIONS];

  // the shared ordered map data structure
  private SortedMap map;

  public ConcurrentSearchBenchmark(int warmup_iterations, int timing_iterations,
      float max_cov, int num_threads, int key_range, int num_operations,
      float percent_insert, float percent_remove, float percent_search,
      int map_algorithm) {
    this.warmup_iterations = warmup_iterations;
    this.timing_iterations = timing_iterations;
    this.max_cov = max_cov;
    this.num_threads = num_threads;
    this.key_range = key_range;
    this.num_operations = num_operations;
    this.percent_insert = percent_insert;
    this.percent_remove = percent_remove;
    this.percent_search = percent_search;
    this.map_algorithm = map_algorithm;

    assert warmup_iterations + timing_iterations < MAX_ITERATIONS;
    assert max_cov > 0.0;
    assert percent_insert + percent_remove + percent_search == 1.0f;

    for ( int i = 0; i < MAX_ITERATIONS; i++ ) {
      times[i] = 0;
    }
    reset();
  }
  
  private void reset() {
    switch ( map_algorithm ) {
      case 0:
        map = new ConcurrentSkipListMap<Integer, Integer>();
        break;
      case 1:
        map = new SnapTreeMap<Integer, Integer>();
        break;
      default:
        System.out.println("Unknown map algorithm " + map_algorithm);
        usage();
        System.exit(1);
        break;
    }
  }

  public void run() {
    ThreadLocalRandom prng = ThreadLocalRandom.current();
    for ( int i = 0; i < num_operations/num_threads; i++ ) {
      double op = prng.nextDouble();
      int key = prng.nextInt(key_range);
      if ( op < percent_insert ) {
        map.put(key, i);
      } else if ( op < percent_insert + percent_remove ) {
        map.remove(key);
      } else {
        map.get(key);
      }
    }
  }

  public void benchmark() {
    Thread threads[] = new Thread[num_threads];
    for ( int iteration = 0; iteration < MAX_ITERATIONS; iteration++ ) {
      for ( int t = 0; t < num_threads; t++ ) {
        threads[t] = new Thread(this);
      }
      long start_time = System.nanoTime();
      for ( int t = 0; t < num_threads; t++ ) {
        threads[t].start();
      }
      for ( int t = 0; t < num_threads; t++ ) {
        try {
          threads[t].join();
        } catch (InterruptedException e) {
          e.printStackTrace();
          System.exit(1);
        }
      }
      long stop_time = System.nanoTime();
      times[iteration] = times[iteration] + stop_time - start_time;
      System.out.println(times[iteration]);
      reset();
    }
  }

  public static void main(String[] args) {

    /* The CLI has the following required positional arguments:
     *   warmup_iterations
     *   timing_iterations
     *   max_cov
     *   num_threads
     *   key_range
     *   num_operations
     *   percent_insert
     *   percent_remove
     *   percent_search
     *   map_algorithm
     */
    int warmup_iterations = 0;
    int timing_iterations = 0;
    float max_cov = 0;
    int num_threads = 0;
    int key_range = 0;
    int num_operations = 0;
    float percent_insert = 0;
    float percent_remove = 0;
    float percent_search = 0;
    int map_algorithm = 0;
    if ( args.length < NUM_ARGS ) {
      usage();
      System.exit(1);
    }

    try {
      warmup_iterations = Integer.parseInt(args[0]);
      timing_iterations = Integer.parseInt(args[1]);
      max_cov = Float.parseFloat(args[2]);
      num_threads = Integer.parseInt(args[3]);
      key_range = Integer.parseInt(args[4]);
      num_operations = Integer.parseInt(args[5]);
      percent_insert = Float.parseFloat(args[6]);
      percent_remove = Float.parseFloat(args[7]);
      percent_search = Float.parseFloat(args[8]);
      map_algorithm = Integer.parseInt(args[9]);
    } catch (Exception e) {
      e.printStackTrace();
      usage();
      System.exit(1);
    }

    ConcurrentSearchBenchmark B = new ConcurrentSearchBenchmark(
        warmup_iterations, timing_iterations,
        max_cov, num_threads, key_range, num_operations,
        percent_insert, percent_remove, percent_search,
        map_algorithm);
    B.benchmark();
  }

  public static void usage() {
    System.out.println("Usage: java ConcurrentSearchBenchmark [args]\n" +
        "\twhere [args] are the following required positional arguments:\n" +
        "\t\twarmup_iterations\n" +
        "\t\ttiming_iterations\n" +
        "\t\tmax_cov\n" +
        "\t\tnum_threads\n" +
        "\t\tkey_range\n" +
        "\t\tnum_operations\n" +
        "\t\tpercent_insert\n" +
        "\t\tpercent_remove\n" +
        "\t\tpercent_search\n" +
        "\t\tmap_algorithm\n"); 
  }


}
