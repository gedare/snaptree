/*
 * Copyright (c) 2014 Gedare Bloom.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

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
  
  /* reset
   * Resets some of this object's state, invoked each benchmark iteration
   * and by the constructor.
   */
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

  /* computeMean
   * Arithmetic mean of times[] between start and stop, inclusive.
   */
  private double computeMean(int start, int stop) {
    double mean = 0;
    for(int j = start; j <= stop; j++) {
      mean = mean + times[j];
    }
    return mean/(stop-start+1);
  }

  /* computeCoV
   * Calculates the coefficient of variation (CoV) for times[] between
   * start and stop, inclusive. CoV is the ratio of the
   * sample standard deviation (s) to the sample mean (m): s/m
   */
  private double computeCoV(int start, int stop) {
    double m = computeMean(start, stop);
    double s = 0;
    for(int j = start; j <= stop; j++) {
      s = s + (times[j] - m)*(times[j] - m);
    }
    s = Math.sqrt(s/(stop-start));
    return s/m;
  }

  /* areWeThereYet
   * Returns the starting iteration for timing_iterations consecutive
   * results with a CoV less than stop_cov, or with the smallest CoV if
   * MAX_ITERATIONS is reached.
   */
  private int areWeThereYet(int iteration, double stop_cov) {
    // skip the warmup
    if ( iteration < warmup_iterations + timing_iterations )
      return 0;
    
    int start = iteration - timing_iterations;
    int stop = iteration;
    double cov = computeCoV(start, stop);
    if(cov < stop_cov ) {
      return start;
    }

    // If the max number of iterations is reached, report the smallest CoV
    // for timing_iterations consecutive results past the warmup
    if (iteration == MAX_ITERATIONS - 1) {
      for (int i = warmup_iterations + timing_iterations; i <= iteration; i++) {
        stop = areWeThereYet(i, stop_cov + 0.01);
        if ( stop > 0 )
          return stop;
      }
    }

    return 0; 
  }

  /* run
   * Thread entry point for benchmark threads, the run function does 
   * the num_operations/num_threads amount of work according to the percent
   * parameters.
   */
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

  /* benchmark
   * In a loop that executes up to MAX_ITERATIONS times, the benchmark
   * method Creates threads to execute the benchmark workload and times the
   * completion time for all threads. If the times for timing_iterations
   * consecutive iterations past the first warmup_iterations has a CoV less
   * than max_cov, the benchmark terminates and reports results. If
   * MAX_ITERATIONS is reached, the smallest CoV achieved for timing_iterations
   * is reported along with the timing results.
   */
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

      int start = areWeThereYet(iteration, max_cov);
      if ( start > 0 ) {
        int stop = start + timing_iterations;
        double cov = computeCoV(start, stop);
        double mm = computeMean(start, stop);
        System.out.println("CoV: " + cov + "\tmean: " + mm + "\tstop: " + stop);
        System.exit(0);
      }
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
    // these parameters are generic for Java benchmarking
    int warmup_iterations = 0;
    int timing_iterations = 0;
    float max_cov = 0;
    int num_threads = 0;

    // the rest are specific to the search benchmark
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
