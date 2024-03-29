package it.unimi.dsi.webgraph.algo;

/*		 
 * Copyright (C) 2010-2013 Sebastiano Vigna 
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */

import it.unimi.dsi.Util;
import it.unimi.dsi.webgraph.algo.EliasFanoCumulativeOutdegreeList;
import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.booleans.BooleanArrays;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.ints.AbstractInt2DoubleFunction;
import it.unimi.dsi.fastutil.ints.Int2DoubleFunction;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.longs.LongBigList;
import it.unimi.dsi.io.SafelyCloseable;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.HyperLogLogCounterArray;
import it.unimi.dsi.util.KahanSummation;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.GraphClassParser;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.NodeIterator;
import it.unimi.dsi.webgraph.Transform;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

/** <p>Computes an approximation of the neighbourhood function, of the size of the reachable sets,
 * and of (discounted) positive geometric centralities of a graph using HyperBall.
 * 
 * <p>HyperBall is an algorithm computing by dynamic programming an approximation 
 * of the sizes of the balls of growing radius around the nodes of a graph. Starting from
 * these data, it can approximate the <em>neighbourhood function</em> of a graph, that is, the function returning
 * for each <var>t</var> the number of pairs of nodes at distance at most <var>t</var>,
 * the number of nodes reachable from each node, Bavelas's closeness centrality, Lin's index, and 
 * <em>harmonic centrality</em> (described by Paolo Boldi and Sebastiano Vigna in &ldquo;Axioms for Centrality&rdquo;, 2013).
 * HyperBall can also compute <em>discounted centralities</em>, in which the weight assigned to a node is some
 * specified function of its distance. All centralities are computed in their <em>positive</em> version (i.e.,
 * using distance <em>from</em> the source: see below how to compute the more usual, and useful, <em>negative</em> version).
 *  
 * <p>HyperBall has been described by Paolo Boldi and Sebastiano Vigna in 
 * &ldquo;In-Core Computation of Geometric Centralities with HyperBall: A Hundred Billion Nodes and Beyond&rdquo;, 2013,
 * and it is a generalization of the method described in &ldquo;HyperANF: Approximating the Neighbourhood Function of Very Large Graphs
 * on a Budget&rdquo;, by Paolo Boldi, Marco Rosa and Sebastiano Vigna,
 * <i>Proceedings of the 20th international conference on World Wide Web</i>, pages 625&minus;634, ACM, (2011).  
 * 
 * <p>Incidentally, HyperBall (actually, HyperANF) has been used to show that Facebook has just <a href="http://vigna.dsi.unimi.it/papers.php#BBRFDS">four degrees of separation</a>.
 *   
 * <p>At step <var>t</var>, for each node we (approximately) keep track (using {@linkplain HyperLogLogCounterArray HyperLogLog counters})
 * of the set of nodes at distance at most <var>t</var>. At each iteration, the sets associated with the successors of each node are merged,
 * thus obtaining the new sets. A crucial component in making this process efficient and scalable is the usage of
 * <em>broadword programming</em> to implement the join (merge) phase, which requires maximising in parallel the list of registers associated with 
 * each successor (the implementation is geared towards 64-bits processors). 
 * 
 * <p>Using the approximate sets, for each <var>t</var> we estimate the number of pairs of nodes (<var>x</var>,<var>y</var>) such
 * that the distance from <var>x</var> to <var>y</var> is at most <var>t</var>. Since during the computation we are also
 * in possession of the number of nodes at distance <var>t</var> &minus; 1, we can also perform computations
 * using the number of nodes at distance <em>exactly</em> <var>t</var> (e.g., centralities).
 * 
 * <p>To use this class, you must first create an instance.
 * Then, you call {@link #init()} (once) and then {@link #iterate()} as much as needed (you can init/iterate several times, if you want so). 
 * Finally, you {@link #close()} the instance. The method {@link #modified()} will tell you whether the internal state of
 * the algorithm has changed. A {@linkplain #run(long, double) commodity method} will do everything for you.
 * 
 * <p>If you additionally pass to the constructor (or on the command line) the <em>transpose</em> of your graph (you can compute it using {@link Transform#transpose(ImmutableGraph)}
 * or {@link Transform#transposeOffline(ImmutableGraph, int)}), when three quarters of the nodes stop changing their value
 * HyperBall will switch to a <em>systolic</em> computation: using the transpose, when a node changes it will signal back
 * to its predecessors that at the next iteration they could change. At the next scan, only the successors of 
 * signalled nodes will be scanned. In particular, 
 * when a very small number of nodes is modified by an iteration, HyperBall will switch to a systolic <em>local</em> mode,
 * in which all information about modified nodes is kept in (traditional) dictionaries, rather than being represented as arrays of booleans.
 * This strategy makes the last phases of the computation significantly faster, and makes
 * in practice the running time of HyperBall proportional to the theoretical bound
 * <i>O</i>(<var>m</var> log <var>n</var>), where <var>n</var>
 * is the number of nodes and <var>m</var> is the number of the arcs of the graph. 
 * 
 * <p>Deciding when to stop iterating is a rather delicate issue. The only safe way is to iterate until {@link #modified()} is zero,
 * and systolic (local) computation makes this goal easily attainable.
 * However, in some cases one can assume that the graph is not pathological, and stop when the relative increment of the number of pairs goes below
 * some threshold.
 *
 * <h2>Computing Centralities</h2>
 * 
 * <p>Note that usually one is interested in the <em>negative</em> version of a centrality measure, that is, the version
 * that depends on the <em>incoming</em> arcs. HyperBall can compute only <em>positive</em> centralities: if you are
 * interested (as it usually happens) in the negative version, you must pass to HyperBall the <em>transpose</em> of the graph
 * (and if you want to run in systolic mode, the original graph, which is the transpose of the transpose). Note that the
 * neighbourhood function of the transpose is identical to the neighbourhood function of the original graph, so the exchange
 * does not alter its computation.
 *
 * <h2>Performance issues</h2>
 * 
 * <p>HyperLogLog counters will be stored in the old generation. If you have a large amount of memory,
 * you will not be able to exploit it fully because the young generation will occupy a significant fraction.
 * In this case, you can tune the size of the new generation (for example, <samp>-XX:MaxNewSize=4G</samp>).
 * Check the garbage collector logs (<samp>-verbose:gc -Xloggc:gc.log</samp>) to be sure that you are 
 * not setting the new generation size too low, yield too frequent minor collections.
 * 
 * <p>This class can perform <em>external</em> computations: instead of keeping in core memory 
 * an old and a new copy of the counters, it can dump on disk an <em>update list</em> containing pairs &lt;<var>node</var>,&nbsp;<var>counter</var>>.
 * At the end of an iteration, the update list is loaded and applied to the counters in memory.
 * The process is of course slower, but the core memory used is halved.
 * 
 * <p>If there are several available cores, the runs of {@link #iterate()} will be <em>decomposed</em> into relatively
 * small tasks (small blocks of nodes) and each task will be assigned to the first available core. Since all tasks are completely
 * independent, this behaviour ensures a very high degree of parallelism. Be careful, however, because this feature requires a graph with
 * a reasonably fast random access (e.g., in the case of a {@link BVGraph}, short reference chains), as many
 * calls to {@link ImmutableGraph#nodeIterator(int)} will be made. The <em>granularity</em> of the decomposition
 * is the number of nodes assigned to each task.
 * 
 * <p>In any case, when attacking very large graphs (in particular, in external mode) some system tuning (e.g.,
 * increasing the filesystem commit time) is a good idea. Also experimenting with granularity and buffer sizes
 * can be useful. Smaller buffers reduce the waits on I/O calls, but increase the time spent in disk seeks.
 * Large buffers improve I/O, but they use a lot of memory. The best possible setup is the one in which 
 * the cores are 100% busy during the graph scan, and the I/O time
 * logged at the end of a scan is roughly equal to the time that is necessary to reload the counters from disk:
 * in such a case, essentially, you are computing as fast as possible.
 * 
 * @author Sebastiano Vigna
 * @author Paolo Boldi
 * @author Marco Rosa
 */

public class HyperBall extends HyperLogLogCounterArray implements SafelyCloseable {
	private static final Logger LOGGER = LoggerFactory.getLogger( HyperBall.class );
	private static final boolean ASSERTS = false;
	private static final long serialVersionUID = 1L;
	
	/** An abstract discount function is a facility to implement a discount function (so that only
	 *  the {@link #get(int)} method must be actually implemented). 
	 *  
	 *  <p>Note that by contract {@link #get(int)} will never be called with argument (i.e., distance) zero.
	 */
	public static abstract class AbstractDiscountFunction extends AbstractInt2DoubleFunction {
		private static final long serialVersionUID = 1L;
		@Override
		public int size() { return -1; }
		@Override
		public boolean containsKey( int key ) { return true; }		
	};
	
	public static Int2DoubleFunction INV_SQUARE_DISCOUNT = new AbstractDiscountFunction() {
		private static final long serialVersionUID = 1L;
		public double get( int distance ) { return 1. / ( (long)distance * distance ); }		
	};

	public static Int2DoubleFunction INV_LOG_DISCOUNT = new AbstractDiscountFunction() { 
		private static final long serialVersionUID = 1L;
		public double get( int distance ) { return 1 / Fast.log2( distance + 1 ); } 
	};
		
	/** The default granularity of a task. */
	public static final int DEFAULT_GRANULARITY = 16 * 1024;
	/** The default size of a buffer in bytes. */
	public static final int DEFAULT_BUFFER_SIZE = 4 * 1024 * 1024;
	/** True if we have the tranpose graph. */
	protected final boolean gotTranpose;
	/** True if we started a systolic computation. */
	protected boolean systolic;
	/** True if we are preparing a local computation (we are {@link #systolic} and less than 1% nodes were modified). */
	protected boolean preLocal;
	/** True if we started a local computation. */
	protected boolean local;
	/** A cached copy of the bit vectors used for counting. */
	protected final long bits[][];
	/** Whether the sum of distances from each node (inverse of <strong>positive</strong> closeness centrality) should be computed; if false, {@link #sumOfDistances} is <code>null</code>. */
	protected  final boolean doSumOfDistances;
	/** Whether the sum of inverse distances from each node (<strong>positive</strong> harmonic centrality) should be computed; if false, {@link #sumOfInverseDistances} is <code>null</code>. */
	protected boolean doSumOfInverseDistances;
	/** The neighbourhood function, if requested. */
	public final DoubleArrayList neighbourhoodFunction;
	/** The sum of the distances from every given node, if requested. */
	public final float[] sumOfDistances;
	/** The sum of inverse distances from each given node, if requested. */
	public final float[] sumOfInverseDistances;
	/** A number of discounted centralities to be computed, possibly none. */
	public final Int2DoubleFunction[] discountFunction;
	/** The overall discounted centrality, for every {@link #discountFunction}. */
	public final float[][] discountedCentrality;
	/** Whether counters are aligned to longwords. */
	protected final boolean longwordAligned;
	/** A mask for the residual bits of a counter (the {@link #counterSize} <code>%</code> {@link Long#SIZE} lowest bits). */
	protected final long counterResidualMask;
	/** The number of nodes of the graph, cached. */
	protected final int numNodes;
	/** The number of arcs of the graph, cached. */
	protected long numArcs;
	/** The square of {@link #numNodes}, cached. */
	protected final double squareNumNodes;
	/** The number of cores used in the computation. */
	protected final int numberOfThreads;
	/** The size of an I/O buffer, in counters. */
	protected final int bufferSize;
	/** The number of actually scanned nodes per task in a multithreaded environment. <strong>Must</strong> be a multiple of {@link Long#SIZE}. */
	protected final int granularity;
	/** The number of nodes per task (obtained by adapting {@link #granularity} to the current ratio of modified nodes). <strong>Must</strong> be a multiple of {@link Long#SIZE}. */
	protected int adaptiveGranularity;
	/** The value computed by the last iteration. */
	protected double last;
	/** The value computed by the current iteration. */
	protected double current;
	/** The current iteration. */
	protected int iteration;
	/** If {@link #external} is true, the name of the temporary file that will be used to write the update list. */
	protected final File updateFile;
	/** If {@link #external} is true, a file channel used to write to the update list. */
	protected final FileChannel fileChannel;
	/** If {@link #external} is true, the random-access file underlying {@link #fileChannel}. */
	protected RandomAccessFile randomAccessFile;
	/** The cumulative list of outdegrees. */
	protected final EliasFanoCumulativeOutdegreeList cumulativeOutdegrees;
	/** A progress logger, or <code>null</code>. */
	protected final ProgressLogger pl;
	/** The lock protecting all critical sections. */
	protected final ReentrantLock lock;
	/** A condition that is notified when all iteration threads are waiting to be started. */
	protected final Condition allWaiting;
	/** The condition on which all iteration threads wait before starting a new phase. */
	protected final Condition start;
	/** A mask containing a one in the most significant bit of each register (i.e., in positions of the form {@link #registerSize registerSize * (i + 1) - 1}). */
	protected final long[] msbMask;
	/** A mask containing a one in the least significant bit of each register (i.e., in positions of the form {@link #registerSize registerSize * i}). */
	protected final long[] lsbMask;
	/** The current computation phase. */
	public int phase;
	/** Whether this approximator has been already closed. */ 
	protected boolean closed;
	/** The threads performing the computation. */
	protected final IterationThread thread[];
	/** An atomic integer keeping track of the number of node processed so far. */
	protected final AtomicInteger nodes;
	/** An atomic integer keeping track of the number of arcs processed so far. */
	protected final AtomicLong arcs;
	/** A variable used to wait for all threads to complete their iteration. */
	protected volatile int aliveThreads;
	/** True if the computation is over. */
	protected volatile boolean completed;
	/** Total number of write operation performed on {@link #fileChannel}. */
	protected volatile long numberOfWrites;
	/** Total wait time in milliseconds of I/O activity on {@link #fileChannel}. */
	protected volatile long totalIoMillis;
	/** The starting node of the next chunk of nodes to be processed. */
	protected int nextNode;
	/** The number of arcs before {@link #nextNode}. */
	protected long nextArcs;
	/** The number of register modified by the last call to {@link #iterate()}. */
	protected final AtomicInteger modified;
	/** Counts the number of unwritten entries when {@link #external} is true, or
	 * the number of counters that did not change their value. */
	protected final AtomicInteger unwritten;
	/** The relative increment of the neighbourhood function for the last iteration. */
	protected double relativeIncrement;
	/** The size of a counter in longwords (ceiled if there are less then {@link Long#SIZE} registers per counter). */
	protected final int counterLongwords;
	/** Whether we should used an update list on disk, instead of computing results in core memory. */
	protected boolean external;
	/** If {@link #external} is false, the arrays where results are stored. */
	protected final long[][] resultBits;
	/** If {@link #external} is false, bit vectors wrapping {@link #resultBits}. */
	protected final LongArrayBitVector[] resultBitVector;
	/** If {@link #external} is false, a {@link #registerSize}-bit views of {@link #resultBits}. */
	protected final LongBigList resultRegisters[];
	/** For each counter, whether it has changed its value. We use an array of boolean (instead of a {@link LongArrayBitVector}) just for access speed. */
	protected boolean[] modifiedCounter;
	/** For each newly computed counter, whether it has changed its value. {@link #modifiedCounter}
	 * will be updated with the content of this bit vector by the end of the iteration. */
	protected boolean[] modifiedResultCounter;
	/** For each counter, whether it has changed its value. We use an array of boolean (instead of a {@link LongArrayBitVector}) just for access speed. */
	protected boolean[] nextMustBeChecked;
	/** For each newly computed counter, whether it has changed its value. {@link #modifiedCounter}
	 * will be updated with the content of this bit vector by the end of the iteration. */
	protected boolean[] mustBeChecked;
	/** If {@link #local} is true, the list of nodes that should be scanned. */
	protected int[] localCheckList;
	/** If {@link #local} is true, the list of nodes that should be scanned on the next iteration. Note that this set is synchronized. */
	protected final IntSet localNextMustBeChecked;
	/** One of the throwables thrown by some of the threads, if at least one thread has thrown a throwable. */
	protected volatile Throwable threadThrowable;
	
	protected final static int ensureRegisters( final int log2m ) {
		if ( log2m < 4 ) throw new IllegalArgumentException( "There must be at least 16 registers per counter" );
		if ( log2m > 60 ) throw new IllegalArgumentException( "There can be at most 2^60 registers per counter" );
		return log2m;
	}
	
	/** Computes the number of threads.
	 * 
	 * <p>If the specified number of threads is zero, {@link Runtime#availableProcessors()} will be returned.
	 * 
	 * @param suggestedNumberOfThreads
	 * @return the actual number of threads.
	 */
	private final static int numberOfThreads( final int suggestedNumberOfThreads ) {
		if ( suggestedNumberOfThreads != 0 ) return suggestedNumberOfThreads;
		return Runtime.getRuntime().availableProcessors();
	}

	/** Creates a new HyperBall instance.
	 * 
	 * @param g the graph whose neighbourhood function you want to compute.
	 * @param gt the tranpose of <code>g</code> in case you want to perform systolic computations, or <code>null</code>.
	 * @param log2m the logarithm of the number of registers per counter.
	 * @param pl a progress logger, or <code>null</code>.
	 * @param numberOfThreads the number of threads to be used (0 for automatic sizing).
	 * @param bufferSize the size of an I/O buffer in bytes (0 for {@link #DEFAULT_BUFFER_SIZE}).
	 * @param granularity the number of node per task in a multicore environment (it will be rounded to the next multiple of 64), or 0 for {@link #DEFAULT_GRANULARITY}.
	 * @param external if true, results of an iteration will be stored on disk.
	 */
	public HyperBall( final ImmutableGraph g, final ImmutableGraph gt, final int log2m, final ProgressLogger pl, final int numberOfThreads, final int bufferSize, final int granularity, final boolean external ) throws IOException {
		this( g, gt, log2m, pl, numberOfThreads, bufferSize, granularity, external, false, false, null, Util.randomSeed() );
	}

	/** Creates a new HyperBall instance using default values.
	 * 
	 * @param g the graph whose neighbourhood function you want to compute.
	 * @param gt the tranpose of <code>g</code> in case you want to perform systolic computations, or <code>null</code>.
	 * @param log2m the logarithm of the number of registers per counter.
	 */
	public HyperBall( final ImmutableGraph g, final ImmutableGraph gt, final int log2m ) throws IOException {
		this( g, gt, log2m, null, 0, 0, 0, false );
	}

	/** Creates a new HyperBall instance using default values.
	 * 
	 * @param g the graph whose neighbourhood function you want to compute.
	 * @param gt the tranpose of <code>g</code> in case you want to perform systolic computations, or <code>null</code>.
	 * @param log2m the logarithm of the number of registers per counter.
	 * @param pl a progress logger, or <code>null</code>.
	 */
	public HyperBall( final ImmutableGraph g, final ImmutableGraph gt, final int log2m, final ProgressLogger pl ) throws IOException {
		this( g, null, log2m, pl, 0, 0, 0, false );
	}

	/** Creates a new HyperBall instance using default values and disabling systolic computation.
	 * 
	 * @param g the graph whose neighbourhood function you want to compute.
	 * @param log2m the logarithm of the number of registers per counter.
	 */
	public HyperBall( final ImmutableGraph g, final int log2m ) throws IOException {
		this( g, null, log2m );
	}

	/** Creates a new HyperBall instance using default values and disabling systolic computation.
	 * 
	 * @param g the graph whose neighbourhood function you want to compute.
	 * @param log2m the logarithm of the number of registers per counter.
	 * @param seed the random seed passed to {@link HyperLogLogCounterArray#HyperLogLogCounterArray(long, long, int, long)}.
	 */
	public HyperBall( final ImmutableGraph g, final int log2m, final long seed ) throws IOException {
		this( g, null, log2m, null, 0, 0, 0, false, false, false, null, seed );
	}

	/** Creates a new HyperBall instance using default values and disabling systolic computation.
	 * 
	 * @param g the graph whose neighbourhood function you want to compute.
	 * @param log2m the logarithm of the number of registers per counter.
	 * @param pl a progress logger, or <code>null</code>.
	 */
	public HyperBall( final ImmutableGraph g, final int log2m, final ProgressLogger pl ) throws IOException {
		this( g, null, log2m, pl );
	}

	
	/** Creates a new HyperBall instance.
	 * 
	 * @param g the graph whose neighbourhood function you want to compute.
	 * @param gt the tranpose of <code>g</code>, or <code>null</code>.
	 * @param log2m the logarithm of the number of registers per counter.
	 * @param pl a progress logger, or <code>null</code>.
	 * @param numberOfThreads the number of threads to be used (0 for automatic sizing).
	 * @param bufferSize the size of an I/O buffer in bytes (0 for {@link #DEFAULT_BUFFER_SIZE}).
	 * @param granularity the number of node per task in a multicore environment (it will be rounded to the next multiple of 64), or 0 for {@link #DEFAULT_GRANULARITY}.
	 * @param external if true, results of an iteration will be stored on disk.
	 * @param doSumOfDistances whether the sum of distances from each node should be computed.
	 * @param doSumOfInverseDistances whether the sum of inverse distances from each node should be computed.
	 * @param discountFunction an array (possibly <code>null</code>) of discount functions. 
	 * @param seed the random seed passed to {@link HyperLogLogCounterArray#HyperLogLogCounterArray(long, long, int, long)}.
	 */
	public HyperBall( final ImmutableGraph g, final ImmutableGraph gt, final int log2m, final ProgressLogger pl, 
			final int numberOfThreads, final int bufferSize, final int granularity, final boolean external, 
			final boolean doSumOfDistances, final boolean doSumOfInverseDistances, final Int2DoubleFunction[] discountFunction, final long seed ) throws IOException {
		super( g.numNodes(), g.numNodes(), ensureRegisters( log2m ), seed );

		info( "Seed : " + Long.toHexString( seed ) );

		gotTranpose = gt != null;
		bits = new long[ bitVector.length ][];
		for( int i = bits.length; i-- != 0; ) bits[ i ] = bitVector[ i ].bits();
		localNextMustBeChecked = gotTranpose ? IntSets.synchronize( new IntOpenHashSet( Hash.DEFAULT_INITIAL_SIZE, Hash.VERY_FAST_LOAD_FACTOR ) ) : null;
		
		numNodes = g.numNodes();
		try {
			numArcs = g.numArcs();
		}
		catch( UnsupportedOperationException e ) {
			// No number of arcs. We have to enumerate.
			long a = 0;
			final NodeIterator nodeIterator = g.nodeIterator();
			for( int i = g.numNodes(); i-- != 0; ) {
				nodeIterator.nextInt();
				a += nodeIterator.outdegree();
			}
			numArcs = a;
		}
		squareNumNodes = (double)numNodes * numNodes;

		cumulativeOutdegrees = new EliasFanoCumulativeOutdegreeList( g, numArcs, Math.max( 0, 64 / m - 1 ) );

		modifiedCounter = new boolean[ numNodes ];
		modifiedResultCounter = external ? null : new boolean[ numNodes ];
		if ( gt != null ) {
			mustBeChecked = new boolean[ numNodes ];
			nextMustBeChecked = new boolean[ numNodes ];
		}

		counterLongwords = ( counterSize + Long.SIZE - 1 ) / Long.SIZE;
		counterResidualMask = ( 1L << counterSize % Long.SIZE ) - 1;
		longwordAligned = counterSize % Long.SIZE == 0;
		
		this.pl = pl;
		this.external = external;
		this.doSumOfDistances = doSumOfDistances;
		this.doSumOfInverseDistances = doSumOfInverseDistances;
		this.discountFunction = discountFunction == null ? new Int2DoubleFunction[ 0 ] : discountFunction;
		this.numberOfThreads = numberOfThreads( numberOfThreads );
		this.granularity = numberOfThreads == 1 ? numNodes : granularity == 0 ? DEFAULT_GRANULARITY : ( ( granularity + Long.SIZE - 1 ) & ~( Long.SIZE - 1 ) );
		this.bufferSize = Math.max( 1, ( bufferSize == 0 ? DEFAULT_BUFFER_SIZE : bufferSize ) / ( ( Long.SIZE / Byte.SIZE ) * ( counterLongwords + 1 ) ) ); 
		
		info( "Relative standard deviation: " + Util.format( 100 * HyperLogLogCounterArray.relativeStandardDeviation( log2m ) ) + "% (" + m  + " registers/counter, " + registerSize + " bits/register, " + Util.format( m * registerSize / 8. ) + " bytes/counter)" );
		if ( external ) info( "Running " + this.numberOfThreads + " threads with a buffer of " + Util.formatSize( this.bufferSize ) + " counters" );
		else info( "Running " + this.numberOfThreads + " threads" );

		thread = new IterationThread[ this.numberOfThreads ];
		
		if ( external ) {
			info( "Creating update list..." );
			updateFile = File.createTempFile( HyperBall.class.getName(), "-temp" );
			updateFile.deleteOnExit();	
			fileChannel = ( randomAccessFile = new RandomAccessFile( updateFile, "rw" ) ).getChannel();
		}
		else {
			updateFile = null;
			fileChannel = null;
		}

		// We initialise the masks for the broadword code in max().
		msbMask = new long[ counterLongwords ];
		lsbMask = new long[ counterLongwords ];
		for( int i = registerSize - 1; i < msbMask.length * Long.SIZE; i += registerSize ) msbMask[ i / Long.SIZE ] |= 1L << i % Long.SIZE; 
		for( int i = 0; i < lsbMask.length * Long.SIZE; i += registerSize ) lsbMask[ i / Long.SIZE ] |= 1L << i % Long.SIZE;
		
		nodes = new AtomicInteger();
		arcs = new AtomicLong();
		modified = new AtomicInteger();
		unwritten = new AtomicInteger();

		neighbourhoodFunction = new DoubleArrayList();
		sumOfDistances = doSumOfDistances ? new float[ numNodes ] : null;
		sumOfInverseDistances = doSumOfInverseDistances ? new float[ numNodes ] : null;
		discountedCentrality = new float[ this.discountFunction.length ][];
		for ( int i = 0; i < this.discountFunction.length; i++ ) discountedCentrality[ i ] = new float[ numNodes ];
		
		if ( ! external ) {
			info( "Allocating result bit vectors..." );
			// Allocate vectors that will store the result.
			resultBitVector = new LongArrayBitVector[ bitVector.length ];
			resultBits = new long[ bitVector.length ][];
			resultRegisters = new LongBigList[ bitVector.length ];
			for( int i = bitVector.length; i-- != 0; ) resultRegisters[ i ] = ( resultBitVector[ i ] = LongArrayBitVector.wrap( resultBits[ i ] = new long[ bitVector[ i ].bits().length ] ) ).asLongBigList( registerSize );
		}
		else {
			resultBitVector = null;
			resultBits = null;
			resultRegisters = null;
		}
		
		info( "HyperBall memory usage: " + Util.formatSize2( usedMemory() ) + " [not counting graph(s)]" );
		
		lock = new ReentrantLock();
		allWaiting = lock.newCondition();
		start = lock.newCondition();
		aliveThreads = this.numberOfThreads;
		
		if ( this.numberOfThreads == 1 ) ( thread[ 0 ] = new IterationThread( g, gt, 0 ) ).start();
		else for( int i = 0; i < this.numberOfThreads; i++ ) ( thread[ i ] = new IterationThread( g.copy(), gt != null ? gt.copy() : null, i ) ).start();
		
		// We wait for all threads being read to start.
		lock.lock();
		try {
			if ( aliveThreads != 0 ) allWaiting.await();
		}
		catch ( InterruptedException e ) {
			throw new RuntimeException( e );
		}
		finally {
			lock.unlock();
		}
	}

	private void info( String s ) {
		if ( pl != null ) pl.logger().info( s );
	}
	
	private long usedMemory() {
		long bytes = 0;
		for( long[] a: bits ) bytes += a.length * ( (long)Long.SIZE / Byte.SIZE );
		if ( sumOfDistances != null ) bytes += sumOfDistances.length * ( (long)Float.SIZE / Byte.SIZE );
		if ( sumOfInverseDistances != null ) bytes += sumOfInverseDistances.length * ( (long)Float.SIZE / Byte.SIZE );
		for ( int i = discountFunction.length; i-- != 0; ) bytes += discountedCentrality[ i ].length * ( (long)Float.SIZE / Byte.SIZE );
		if ( resultBits != null ) for( long[] a: resultBits ) bytes += a.length * ( (long)Long.SIZE / Byte.SIZE );
		if ( modifiedCounter != null ) bytes += modifiedCounter.length;
		if ( modifiedResultCounter != null ) bytes += modifiedResultCounter.length;
		if ( nextMustBeChecked != null ) bytes += nextMustBeChecked.length;
		if ( mustBeChecked != null ) bytes += mustBeChecked.length;
		return bytes;
	}

	private void ensureOpen() {
		if ( closed ) throw new IllegalStateException( "This " + HyperBall.class.getSimpleName() + " has been closed." );
	}

	/** Initialises the approximator.
	 * 
	 * <p>This method must be call before a series of {@linkplain #iterate() iterations}.
	 * Note that it will <em>not</em> change the seed used by the underlying {@link HyperLogLogCounterArray}.
	 * 
	 * @see #init(long)
	 */
	public void init() {
		init( seed );
	}
		
	/** Initialises the approximator, providing a new seed to the underlying {@link HyperLogLogCounterArray}.
	 * 
	 * <p>This method must be call before a series of {@linkplain #iterate() iterations}.
	 * @param seed passed to {@link #clear(long)}.
	 */
	public void init( final long seed ) {
		ensureOpen();
		info( "Clearing all registers..." );
		clear( seed );

		// We load the counter i with node i.
		for( int i = numNodes; i-- != 0; ) add( i, i );

		iteration = -1;
		completed = systolic = local = preLocal = false;
		
		if ( ! external ) for( LongArrayBitVector bv: resultBitVector ) bv.fill( false );
		
		if ( sumOfDistances != null ) Arrays.fill( sumOfDistances, 0 );
		if ( sumOfInverseDistances != null ) Arrays.fill( sumOfInverseDistances, 0 );
		for ( int i = 0; i < discountFunction.length; i++ ) Arrays.fill( discountedCentrality[ i ], 0 );
		
		// The initial value (the iteration for this value does not actually happen).
		neighbourhoodFunction.add( last = numNodes );

		BooleanArrays.fill( modifiedCounter, true ); // Initially, all counters are modified.
		
		if ( pl != null ) {
			pl.displayFreeMemory = true;
			pl.itemsName = "iterates";
			pl.start( "Iterating..." );
		}
	}

	public void close() throws IOException {
		if ( closed ) return;
		closed = true;
		
		lock.lock();
		try {
			completed = true;
			start.signalAll();
		}
		finally {
			lock.unlock();
		}

		for( Thread t: thread )
			try {
				t.join();
			}
			catch ( InterruptedException e ) {
				throw new RuntimeException( e );
			}
		
			if ( external ) {
				randomAccessFile.close();
				fileChannel.close();
				updateFile.delete();
			}
	}

	protected void finalize() throws Throwable {
		try {
			if ( ! closed ) {
				LOGGER.warn( "This " + this.getClass().getName() + " [" + toString() + "] should have been closed." );
				close();
			}
		}
		finally {
			super.finalize();
		}
	}


	/** Performs a multiple precision subtraction, leaving the result in the first operand.
	 * 
	 * @param x a vector of longs.
	 * @param y a vector of longs that will be subtracted from <code>x</code>.
	 * @param l the length of <code>x</code> and <code>y</code>.
	 */
	private final static void subtract( final long[] x, final long[] y, final int l ) {
		boolean borrow = false;
		
		for( int i = 0; i < l; i++ ) {			
			if ( ! borrow || x[ i ]-- != 0 ) borrow = x[ i ] < y[ i ] ^ x[ i ] < 0 ^ y[ i ] < 0; // This expression returns the result of an unsigned strict comparison.
			x[ i ] -= y[ i ];
		}	
	}
	
	/** Computes the register-by-register maximum of two bit vectors.
	 * 
	 * @param x first vector of longs, representing a bit vector in {@link LongArrayBitVector} format, where the result will be stored.
	 * @param y a second vector of longs, representing a bit vector in {@link LongArrayBitVector} format, that will be maximised with <code>x</code>.
	 * @param r the register size.
	 */
	
	protected final void max( final long[] x, final long[] y, final int r, final long[] accumulator, final long[] mask ) {
		final int l = x.length;
		final long[] msbMask = this.msbMask;

		/* We work in two phases. Let H_r (msbMask) by the mask with the
		 * highest bit of each register (of size r) set, and L_r (lsbMask) 
		 * be the mask with the lowest bit of each register set. 
		 * We describe the algorithm on a single word.
		 * 
		 * If the first phase we perform an unsigned strict register-by-register 
		 * comparison of x and y, using the formula
		 * 
		 * z = (  ( ((y | H_r) - (x & ~H_r)) | (y ^ x) )^ (y | ~x)  ) & H_r
		 *  
		 * Then, we generate a register-by-register mask of all ones or
		 * all zeroes, depending on the result of the comparison, using the 
		 * formula
		 * 
		 * ( ( (z >> r-1 | H_r) - L_r ) | H_r ) ^ z
		 * 
		 * At that point, it is trivial to select from x and y the right values.
		 */
		
		// We load y | H_r into the accumulator.
		for( int i = l; i-- != 0; ) accumulator[ i ] = y[ i ] | msbMask[ i ]; 
		// We subtract x & ~H_r, using mask as temporary storage
		for( int i = l; i-- != 0; ) mask[ i ] = x[ i ] & ~msbMask[ i ]; 
		subtract( accumulator, mask, l );
		
		// We OR with x ^ y, XOR with ( x | ~y), and finally AND with H_r. 
		for( int i = l; i-- != 0; ) accumulator[ i ] = ( ( accumulator[ i ] | ( y[ i ] ^ x[ i ] ) ) ^ ( y[ i ] | ~x[ i ] ) ) & msbMask[ i ]; 
	
		if ( ASSERTS ) {
			final LongBigList a = LongArrayBitVector.wrap( x ).asLongBigList( r );
			final LongBigList b = LongArrayBitVector.wrap( y ).asLongBigList( r );
			for( int i = 0; i < m; i++ ) {
				long pos = ( i + 1 ) * (long)r - 1; 
				assert ( b.getLong( i ) < a.getLong( i ) ) == ( ( accumulator[ (int)( pos / Long.SIZE ) ] & 1L << pos % Long.SIZE ) != 0 ); 
			}
		}
		
		// We shift by r - 1 places and put the result into mask.
		final int rMinus1 = r - 1, longSizeMinusRMinus1 = Long.SIZE - rMinus1;
		for( int i = l - 1; i-- != 0; ) mask[ i ] = accumulator[ i ] >>> rMinus1 | accumulator[ i + 1 ] << longSizeMinusRMinus1 | msbMask[ i ]; 
		mask[ l - 1 ] = accumulator[ l - 1 ] >>> rMinus1 | msbMask[ l - 1 ];

		// We subtract L_r from mask.
		subtract( mask, lsbMask, l );

		// We OR with H_r and XOR with the accumulator.
		for( int i = l; i-- != 0; ) mask[ i ] = ( mask[ i ] | msbMask[ i ] ) ^ accumulator[ i ];
		
		if ( ASSERTS ) {
			final long[] t = x.clone();
			LongBigList a = LongArrayBitVector.wrap( t ).asLongBigList( r );
			LongBigList b = LongArrayBitVector.wrap( y ).asLongBigList( r );
			for( int i = 0; i < Long.SIZE * l / r; i++ ) a.set( i, Math.max( a.getLong( i ), b.getLong( i ) ) );
			// Note: this must be kept in sync with the line computing the result.
			for( int i = l; i-- != 0; ) assert t[ i ] == ( ~mask[ i ] & x[ i ] | mask[ i ] & y[ i ] );
		}

		// Finally, we use mask to select the right bits from x and y and store the result.
		for( int i = l; i-- != 0; ) x[ i ] ^= ( x[ i ] ^ y[ i ] ) & mask[ i ]; 

	}

	/** Copies a counter to a local array.
	 * 
	 * @param chunkBits the array storing the counter.
	 * @param t a local destination array.
	 * @param node the node number.
	 */
	protected final void copyToLocal( final long[] chunkBits, final long[] t, final int node ) {
		if ( longwordAligned ) System.arraycopy( chunkBits, (int)( offset( node ) / Long.SIZE ), t, 0, counterLongwords );
		else {
			// Offset in bits
			final long offset = offset( node );
			// Offsets in elements in the array
			final int longwordOffset = (int)( offset / Long.SIZE );
			// Offset in bits in the word of index longwordOffset
			final int bitOffset = (int)( offset % Long.SIZE );
			final int last = counterLongwords - 1; 

			if ( bitOffset == 0 ) {
				for( int i = last; i-- != 0; ) t[ i ] = chunkBits[ longwordOffset + i ];
				t[ last ] = chunkBits[ longwordOffset + last ] & counterResidualMask;
			}
			else {
				for( int i = 0; i < last; i++ ) t[ i ] = chunkBits[ longwordOffset + i ] >>> bitOffset | chunkBits[ longwordOffset + i + 1 ] << Long.SIZE - bitOffset;  
				t[ last ] = chunkBits[ longwordOffset + last ] >>> bitOffset & counterResidualMask;
			}
		}
	}
	
	/** Copies a counter from a local array.
	 * @param t a local array.
	 * @param chunkBits the array where the counter will be stored.
	 * @param node the node number.
	 */
	protected final void copyFromLocal( final long[] t, final long[] chunkBits, final int node ) {
		if ( longwordAligned ) System.arraycopy( t, 0, chunkBits, (int)( offset( node ) / Long.SIZE ), counterLongwords );
		else {
			// Offset in bits
			final long offset = offset( node );
			// Offsets in elements in the array
			final int longwordOffset = (int)( offset / Long.SIZE );
			// Offset in bits in the word of index longwordOffset
			final int bitOffset = (int)( offset % Long.SIZE );
			final int last = counterLongwords - 1; 

			if ( bitOffset == 0 ) {
				for( int i = last; i-- != 0; ) chunkBits[ longwordOffset + i ] = t[ i ];
				chunkBits[ longwordOffset + last ] &= ~counterResidualMask;
				chunkBits[ longwordOffset + last ] |= t[ last ] & counterResidualMask;
			}
			else {
				chunkBits[ longwordOffset ] &= ( 1L << bitOffset ) - 1;
				chunkBits[ longwordOffset ] |= t[ 0 ] << bitOffset;
				
				for( int i = 1; i < last; i++ ) chunkBits[ longwordOffset + i ] = t[ i - 1 ] >>> Long.SIZE - bitOffset | t[ i ] << bitOffset; 

				final int remaining = counterSize % Long.SIZE + bitOffset;

				final long mask = -1L >>> ( Long.SIZE - Math.min( Long.SIZE, remaining ) );
				chunkBits[ longwordOffset + last ] &= ~mask;
				chunkBits[ longwordOffset + last ] |= mask & ( t[ last - 1 ] >>> Long.SIZE - bitOffset | t[ last ] << bitOffset );

				// Note that it is impossible to enter in this conditional unless you use 7 or more bits per register, which is unlikely.
				if ( remaining > Long.SIZE ) {
					final long mask2 = ( 1L << remaining - Long.SIZE ) - 1;
					chunkBits[ longwordOffset + last + 1 ] &= ~mask2;
					chunkBits[ longwordOffset + last + 1 ] |= mask2 & ( t[ last ] >>> Long.SIZE - bitOffset );
				}
			}
			
			if ( ASSERTS ) {
				final LongArrayBitVector l = LongArrayBitVector.wrap( chunkBits );
				for( int i = 0; i < counterSize; i++ ) assert l.getBoolean( offset + i ) == ( ( t[ i / Long.SIZE ] & ( 1L << i % Long.SIZE ) ) != 0 ); 
			}
		}
	}
	
	/** Transfers the content of a counter between two parallel array of longwords.
	 * 
	 * @param source the source array.
	 * @param dest the destination array.
	 * @param node the node number.
	 */
	protected final void transfer( final long[] source, final long[] dest, final int node ) {
		if ( longwordAligned ) {
			final int longwordOffset = (int)( offset( node ) / Long.SIZE );
			System.arraycopy( source, longwordOffset, dest, longwordOffset, counterLongwords );
		}
		else { 
			// Offset in bits in the array
			final long offset = offset( node );
			// Offsets in elements in the array
			final int longwordOffset = (int)( offset / Long.SIZE );
			// Offset in bits in the word of index longwordOffset
			final int bitOffset = (int)( offset % Long.SIZE );
			final int last = counterLongwords - 1; 

			if ( bitOffset == 0 ) {
				for( int i = last; i-- != 0; ) dest[ longwordOffset + i ] = source[ longwordOffset + i ];
				dest[ longwordOffset + last ] &= ~counterResidualMask;
				dest[ longwordOffset + last ] |= source[ longwordOffset + last ] & counterResidualMask;
			}
			else {
				final long mask = -1L << bitOffset;
				dest[ longwordOffset ] &= ~mask;
				dest[ longwordOffset ] |= source[ longwordOffset ] & mask;
				
				for( int i = 1; i < last; i++ ) dest[ longwordOffset + i ] = source[ longwordOffset + i ]; 

				final int remaining = ( counterSize + bitOffset ) % Long.SIZE;
				if ( remaining == 0 ) dest[ longwordOffset + last ] = source[ longwordOffset + last ];
				else {
					final long mask2 = ( 1L << remaining ) - 1;
					dest[ longwordOffset + last ] &= ~mask2;
					dest[ longwordOffset + last ] |= mask2 & source[ longwordOffset + last ];
				}
			}

			if ( ASSERTS ) {
				LongArrayBitVector aa = LongArrayBitVector.wrap( source );
				LongArrayBitVector bb = LongArrayBitVector.wrap( dest );
				for( int i = 0; i < counterSize; i++ ) assert aa.getBoolean( offset + i ) == bb.getBoolean( offset + i ); 
			}
		}
	}
	
	private final class IterationThread extends Thread {
		/** A copy of the graph for this thread only. */
		private final ImmutableGraph g;
		/** A copy of the tranpose graph for this thread only. */
		private final ImmutableGraph gt;
		/** The index of this thread (just used to identify the thread). */
		private final int index;
		
		/** Create a new iteration thread.
		 * @param index the index of this thread (just used to identify the thread).
		 */
		private IterationThread( final ImmutableGraph g, ImmutableGraph gt, final int index ) {
			this.g = g;
			this.gt = gt;
			this.index = index;
		}

		private final boolean synchronize( final int phase ) throws InterruptedException {
			lock.lock();
			try {
				if ( --aliveThreads == 0 ) allWaiting.signal();
				if ( aliveThreads < 0 ) throw new IllegalStateException();
				start.await();
				if ( completed ) return true;
				if ( phase != HyperBall.this.phase ) throw new IllegalStateException( "Main thread is in phase " + HyperBall.this.phase + ", but thread " + index + " is heading to phase " + phase );
				return false;
			}
			finally {
				lock.unlock();
			}
		}

		@Override
		public void run() {
			try {
				// Lots of local caching.
				final int registerSize = HyperBall.this.registerSize;
				final int counterLongwords = HyperBall.this.counterLongwords;
				final boolean external = HyperBall.this.external;
				final ImmutableGraph g = this.g;
				final boolean doSumOfDistances = HyperBall.this.doSumOfDistances;
				final boolean doSumOfInverseDistances = HyperBall.this.doSumOfInverseDistances;
				final int numberOfDiscountFunctions = HyperBall.this.discountFunction.length;
				final boolean doCentrality = doSumOfDistances || doSumOfInverseDistances || numberOfDiscountFunctions != 0;

				final long[] accumulator = new long[ counterLongwords ];
				final long[] mask = new long[ counterLongwords ];

				final long t[] = new long[ counterLongwords ];
				final long prevT[] = new long[ counterLongwords ];
				final long u[] = new long[ counterLongwords ];

				final ByteBuffer byteBuffer = external ? ByteBuffer.allocate( ( Long.SIZE / Byte.SIZE ) * bufferSize * ( counterLongwords + 1 ) ) : null;
				if ( external ) byteBuffer.clear();

				for(;;) {
					
					if ( synchronize( 0 ) ) return;

					// These variables might change across executions of the loop body.
					final int granularity = HyperBall.this.adaptiveGranularity;
					final long arcGranularity = (long)Math.ceil( (double)numArcs * granularity / numNodes );
					final long bits[][] = HyperBall.this.bits;
					final long resultBits[][] = HyperBall.this.resultBits;
					final boolean[] modifiedCounter = HyperBall.this.modifiedCounter;
					final boolean[] modifiedResultCounter = HyperBall.this.modifiedResultCounter;
					final boolean[] mustBeChecked = HyperBall.this.mustBeChecked;
					final boolean[] nextMustBeChecked = HyperBall.this.nextMustBeChecked;
					final boolean systolic = HyperBall.this.systolic;
					final boolean local = HyperBall.this.local;
					final boolean preLocal = HyperBall.this.preLocal;
					final int[] localCheckList = HyperBall.this.localCheckList;
					final IntSet localNextMustBeChecked = HyperBall.this.localNextMustBeChecked;
					
					int start = -1;
					int end = -1;
					int modified = 0; // The number of registers that have been modified during the computation of the present task.
					int unwritten = 0; // The number of counters not written to disk.

					// In a local computation tasks are based on the content of localCheckList.
					int upperLimit = local ? localCheckList.length : numNodes;
					
					/* During standard iterations, cumulates the neighbourhood function for the nodes scanned
					 * by this thread. During systolic iterations, cumulates the *increase* of the
					 * neighbourhood function for the nodes scanned by this thread. */
					final KahanSummation neighbourhoodFunctionDelta = new KahanSummation();
					
					for(;;) {

						// Try to get another piece of work.
						synchronized( HyperBall.this.cumulativeOutdegrees ) {
							if ( nextNode == upperLimit ) break;
							start = nextNode;
							if ( local ) nextNode++;
							else { 
								final long target = nextArcs + arcGranularity;
								if ( target >= numArcs ) nextNode = numNodes;
								else {
									nextArcs = cumulativeOutdegrees.skipTo( target );
									nextNode = cumulativeOutdegrees.currentIndex();										
								}
							}
							end = nextNode;
						}						
						
						final NodeIterator nodeIterator = local || systolic ? null : g.nodeIterator( (int)start ); 
						long arcs = 0;
						
						for( int i = start; i < end; i++ ) {
							final int node = local ? localCheckList[ i ] : i;
							/* The three cases in which we enumerate successors:
							 * 1) A non-systolic computation (we don't know anything, so we enumerate).
							 * 2) A systolic, local computation (the node is by definition to be checked, as it comes from the local check list).
							 * 3) A systolic, non-local computation in which the node should be checked. 
							 */
							if ( ! systolic || local || mustBeChecked[ node ] ) {
								int d;
								int[] successor = null;
								LazyIntIterator successors = null;
								
								if ( local || systolic ) {
									d = g.outdegree( node );
									successors = g.successors( node );
								}
								else {
									nodeIterator.nextInt();
									d = nodeIterator.outdegree();
									successor = nodeIterator.successorArray();
								}
								 
								final int chunk = chunk( node );
								copyToLocal( bits[ chunk ], t, node );
								// Caches t's values into prevT
								System.arraycopy( t, 0, prevT, 0, counterLongwords );
								
								boolean counterModified = false;

								for( int j = d; j-- != 0; ) {
									final int s = local || systolic ? successors.nextInt() : successor[ j ];
									/* Neither self-loops nor unmodified counter do influence the computation. Note 
									 * that in local mode we no longer keep track of modified counters. */
									if ( s != node && ( local || modifiedCounter[ s ] ) ) { 
										counterModified = true; // This is just to mark that we entered the loop at least once.
										copyToLocal( bits[ chunk( s ) ], u, s );
										max( t, u, registerSize, accumulator, mask );
									}
								}

								arcs += d;
								
								if ( ASSERTS )  {
									LongBigList test = LongArrayBitVector.wrap( t ).asLongBigList( registerSize );
									for( int rr = 0; rr < m; rr++ ) {
										int max = (int)registers[ chunk( node ) ].getLong( ( (long)node << log2m ) + rr );
										if ( local || systolic ) successors = g.successors( node );
										for( int j = d; j-- != 0; ) {
											final int s = local || systolic ? successors.nextInt() : successor[ j ];
											max = Math.max( max, (int)registers[ chunk( s ) ].getLong( ( (long)s << log2m ) + rr ) );
										}
										assert max == test.getLong( rr ) : max + "!=" + test.getLong( rr ) + " [" + rr + "]";
									}
								}

								if ( counterModified ) {
									/* If we enter this branch, we have maximised with at least one successor.
									 * We must thus check explicitly whether we have modified the counter. */
									counterModified = false;
									for( int p = counterLongwords; p-- != 0; ) 
										if ( prevT[ p ] != t[ p ] ) {
											counterModified = true;
											break;
										}
								}

								double post = Double.NaN;
								
								/* We need the counter value only if the iteration is standard (as we're going to
								 * compute the neighbourhood function cumulating actual values, and not deltas) or
								 * if the counter was actually modified (as we're going to cumulate the neighbourhood
								 * function delta, or at least some centrality). */
								if ( ! systolic || counterModified ) post = count( t, 0 );
								if ( ! systolic ) neighbourhoodFunctionDelta.add( post );
								
								// Here counterModified is true only if the counter was *actually* modified.
								if ( counterModified && ( systolic || doCentrality ) ) {
									final double pre = count( node );
									if ( systolic ) {
										neighbourhoodFunctionDelta.add( -pre );
										neighbourhoodFunctionDelta.add( post );
									}
									
									if ( doCentrality ) {
										final double delta = post - pre;
										// Note that this code is executed only for distances > 0.
										if ( delta > 0 ) { // Force monotonicity
											if ( doSumOfDistances ) sumOfDistances[ node ] += delta * ( iteration + 1 );
											if ( doSumOfInverseDistances ) sumOfInverseDistances[ node ] += delta / ( iteration + 1 );
											for ( int j = numberOfDiscountFunctions; j-- != 0; ) discountedCentrality[ j ][ node ] += delta * discountFunction[ j ].get( iteration + 1 );
										}
									}
								}
								
								if ( counterModified ) {
									/* We keep track of modified counters in the result either if we are preparing
									 * a local computation, or if we are not in external mode (in external mode
									 * modified counters are computed when the update list is reloaded). 
									 * Note that we must add the current node to the must-be-checked set 
									 * for the next iteration if it is modified, as it might need a copy 
									 * to the result array at the next iteration. */
									if ( preLocal ) localNextMustBeChecked.add( node );
									else if ( ! external ) modifiedResultCounter[ node ] = true;

									if ( systolic ) {
										final LazyIntIterator predecessors = gt.successors( node );
										int p;
										/* In systolic computations we must keep track of which counters must
										 * be checked on the next iteration. If we are preparing a local computation,
										 * we do this explicitly, by adding the predecessors of the current
										 * node to a set. Otherwise, we do this implicitly, by setting the
										 * corresponding entry in an array. */
										if ( preLocal ) while( ( p = predecessors.nextInt() ) != -1 ) localNextMustBeChecked.add( p );
										else while( ( p = predecessors.nextInt() ) != -1 ) nextMustBeChecked[ p ] = true;
									}

									modified++;
								}

								if ( external ) {
									if ( counterModified ) {
										byteBuffer.putLong( node );
										for( int p = counterLongwords; p-- != 0; ) byteBuffer.putLong( t[ p ] );

										if ( ! byteBuffer.hasRemaining() ) {
											byteBuffer.flip();
											long time = -System.currentTimeMillis();
											fileChannel.write( byteBuffer );
											time += System.currentTimeMillis();
											totalIoMillis += time;
											numberOfWrites++;
											byteBuffer.clear();
										}
									}
									else unwritten++;
								}
								else {
									/* This is slightly subtle: if a counter is not modified, and
									 * the present value was not a modified value in the first place,
									 * then we can avoid updating the result altogether. In local computations
									 * we must always update, as we do not keep track of modified counters
									 * (but we know that all counters modified in the previous iteration
									 * are in the local check list). */
									if ( counterModified || local || modifiedCounter[ node ] ) copyFromLocal( t, resultBits[ chunk ], node );
									else unwritten++;
								}
							}
							else if ( ! external ) {
								/* Even if we cannot possible have changed our value, still our copy
								 * in the result vector might need to be updated because it does not
								 * reflect our current value. */
								if ( modifiedCounter[ node ] ) {
									final int chunk = chunk( node );
									transfer( bits[ chunk ], resultBits[ chunk ], node );
								}
								else unwritten++;
							}
						}

						// Update the global progress counter.
						HyperBall.this.arcs.addAndGet( arcs );
						nodes.addAndGet( end - (int)start );
					}

					if ( external ) {
						// If we can avoid at all calling FileChannel.write(), we do so.
						if( byteBuffer.position() != 0 ) {
							byteBuffer.flip();
							long time = -System.currentTimeMillis();
							fileChannel.write( byteBuffer );
							time += System.currentTimeMillis();
							totalIoMillis += time;
							numberOfWrites++;
							byteBuffer.clear();
						}
					}

					HyperBall.this.modified.addAndGet( modified );
					HyperBall.this.unwritten.addAndGet( unwritten );

					synchronized( HyperBall.this ) {
						current += neighbourhoodFunctionDelta.value();
					}
					
					if ( external ) {
						synchronize( 1 );						
						/* Read into memory newly computed counters, updating modifiedCounter.
						 * Note that if m is less than 64 copyFromLocal(), being unsynchronised, might
						 * cause race conditions (when maximising each thread writes in a longword-aligned
						 * block of memory, so no race conditions can arise). Since synchronisation would
						 * lead to significant contention (as we cannot synchronise at a level finer than
						 * a bit vector, and update lists might be quite dense and local), we prefer simply
						 * to do the update with thread 0 only. */
						if ( index == 0 || m >= Long.SIZE ) for(;;) {
							byteBuffer.clear();
							if ( fileChannel.read( byteBuffer ) <= 0 ) break;
							byteBuffer.flip();
							while( byteBuffer.hasRemaining() ) {
								final int node = (int)byteBuffer.getLong();
								for( int p = counterLongwords; p-- != 0; ) t[ p ] = byteBuffer.getLong();
								copyFromLocal( t, bits[ chunk( node ) ], node );
								if ( ! preLocal ) modifiedCounter[ node ] = true;
							}
						}
					}
					
				}
			}
			catch( Throwable t ) {
				t.printStackTrace();
				threadThrowable = t;
				lock.lock();
				try {
					if ( --aliveThreads == 0 ) allWaiting.signal();
				}
				finally {
					lock.unlock();
				}
			}
		}
		
		public String toString() {
			return "Thread " + index;
		}
	}
	
	/** Performs a new iteration of HyperBall. */
	public void iterate() throws IOException {
		ensureOpen();
		try {
			iteration++;
						
			// Let us record whether the previous computation was systolic.
			final boolean preSystolic = systolic; 
			
			/* If less than one fourth of the nodes have been modified, and we have the transpose, 
			 * it is time to pass to a systolic computation. */
			systolic = gotTranpose && iteration > 0 && modified.get() < numNodes / 4;

			/* Non-systolic computations add up the value of all counter. 
			 * Systolic computations modify the last value by compensating for each modified counter. */
			current = systolic ? last : 0;

			// If we completed the last iteration in pre-local mode, we MUST run in local mode.
			local = preLocal;
			
			// We run in pre-local mode if we are systolic and few nodes where modified.
			preLocal = systolic && modified.get() < numNodes / 100;

			info( "Starting " + ( systolic ? "systolic iteration (local: " + local + "; pre-local: " + preLocal + ")"  : "standard " + ( external ? "external " : "" ) + "iteration" ) );
			
			if ( local ) {
				/* In case of a local computation, we convert the set of must-be-checked for the 
				 * next iteration into a check list. */
				localCheckList = localNextMustBeChecked.toIntArray();
			}
			else if ( systolic ) {
				// Systolic, non-local computations store the could-be-modified set implicitly into this array.
				BooleanArrays.fill( nextMustBeChecked, false );
				// If the previous computation wasn't systolic, we must assume that all registers could have changed.
				if ( ! preSystolic ) BooleanArrays.fill( mustBeChecked, true );
			}

			if ( preLocal ) localNextMustBeChecked.clear();
			
			if ( ! external && ! preLocal ) BooleanArrays.fill( modifiedResultCounter, false );
			
			adaptiveGranularity = granularity;
			if ( numberOfThreads > 1 ) {
				if ( ! local && iteration > 0 ) {
					adaptiveGranularity = (int)Math.min( Math.max( 1, numNodes / numberOfThreads ), granularity * ( numNodes / Math.max( 1., modified() ) ) );
					adaptiveGranularity = ( adaptiveGranularity + Long.SIZE - 1 ) & ~( Long.SIZE - 1 );
				}
				info( "Adaptive granularity for this iteration: " + adaptiveGranularity );
			}
			
			modified.set( 0 );
			totalIoMillis = 0;
			numberOfWrites = 0;
			final ProgressLogger npl = pl == null ? null : new ProgressLogger( LOGGER, 1, TimeUnit.MINUTES, "arcs" );
			
			if ( npl != null ) {
				arcs.set( 0 );
				npl.expectedUpdates = systolic || local ? -1 : numArcs;
				npl.start( "Scanning graph..." );
			}

			nodes.set( 0 );
			nextArcs = nextNode = 0;
			unwritten.set( 0 );
			if ( external ) fileChannel.position( 0 );

			// Start all threads.
			lock.lock();
			try {
				phase = 0;
				aliveThreads = numberOfThreads;
				start.signalAll();

				// Wait for all threads to complete their tasks, logging some stuff in the mean time.
				while( aliveThreads != 0 ) {
					allWaiting.await( 1, TimeUnit.MINUTES );
					if ( threadThrowable != null ) throw new RuntimeException( threadThrowable );
					final int aliveThreads = this.aliveThreads;
					if ( npl != null && aliveThreads != 0 ) {
						if ( arcs.longValue() != 0 ) npl.set( arcs.longValue() );
						if ( external && numberOfWrites > 0 ) {
							final long time = npl.millis();
							info( "Writes: " + numberOfWrites + "; per second: " + Util.format( 1000.0 * numberOfWrites / time ) );
							info( "I/O time: " + Util.format( ( totalIoMillis / 1000.0 ) ) + "s; per write: " + ( totalIoMillis / 1000.0 ) / numberOfWrites + "s" );
						}
						if ( aliveThreads != 0 ) info( "Alive threads: " + aliveThreads + " (" + Util.format( 100.0 * aliveThreads / numberOfThreads ) + "%)" );
					}
				}
			}
			finally {
				lock.unlock();
			}

			if ( npl != null ) {
				npl.done( arcs.longValue() );
				if ( ! external ) info( "Unwritten counters: " + Util.format( unwritten.intValue() ) + " (" + Util.format( 100.0 * unwritten.intValue() / numNodes ) + "%)" );
				info( "Unmodified counters: " + Util.format( numNodes - modified.intValue() ) + " (" + Util.format( 100.0 * ( numNodes - modified.intValue() ) / numNodes ) + "%)" );
			}

			if ( external ) {
				if ( npl != null ) {
					npl.itemsName = "counters";
					npl.start( "Updating counters..." );
				}

				// Read into memory the newly computed counters.
			
				fileChannel.truncate( fileChannel.position() );
				fileChannel.position( 0 );
				
				// In pre-local mode, we do not clear modified counters.
				if ( ! preLocal ) BooleanArrays.fill( modifiedCounter, false );

				lock.lock();
				try {
					phase = 1;
					//System.err.println( "Starting phase 1..." );
					aliveThreads = numberOfThreads;
					start.signalAll();
					// Wait for all threads to complete the counter update.
					if ( aliveThreads != 0 ) allWaiting.await();
					if ( threadThrowable != null ) throw new RuntimeException( threadThrowable );
				}
				finally {
					lock.unlock();
				}

				if ( npl != null ) {
					npl.count = modified();
					npl.done();
				}
			}
			else {
				// Switch the bit vectors.
				for( int i = 0; i < bitVector.length; i++ ) {
					if ( npl != null ) npl.update( bitVector[ i ].bits().length );
					final LongBigList r = registers[ i ];
					registers[ i ] = resultRegisters[ i ];
					resultRegisters[ i ] = r;
					final LongArrayBitVector v = bitVector[ i ];
					bitVector[ i ] = resultBitVector[ i ];
					resultBitVector[ i ] = v;
					resultBits[ i ] = resultBitVector[ i ].bits();
					bits[ i ] = bitVector[ i ].bits();
				}

				// Switch modifiedCounters and modifiedResultCounters, and fill with zeroes the latter.
				final boolean[] t = modifiedCounter;
				modifiedCounter = modifiedResultCounter;
				modifiedResultCounter = t;
			}
			
			if ( systolic ) {
				// Switch mustBeChecked and nextMustBeChecked, and fill with zeroes the latter.
				final boolean[] t = mustBeChecked;
				mustBeChecked = nextMustBeChecked;
				nextMustBeChecked = t;
			}
			
			/* We enforce monotonicity. Non-monotonicity can only be caused
			 * by approximation errors. */
			if ( current < last ) current = last; 
			relativeIncrement = current / last;
			
			if ( pl != null ) {
				pl.logger().info( "Pairs: " + current + " (" + current * 100.0 / squareNumNodes + "%)"  );
				pl.logger().info( "Absolute increment: " + ( current - last ) );
				pl.logger().info( "Relative increment: " + relativeIncrement );
			}

			neighbourhoodFunction.add( current );
			last = current;

			if ( pl != null ) pl.updateAndDisplay();
		}
		catch ( InterruptedException e ) {
			throw new RuntimeException( e );
		}
	}

	/** Returns the number of HyperLogLog counters that were modified by the last call to {@link #iterate()}.
	 * 
	 * @return the number of HyperLogLog counters that were modified by the last call to {@link #iterate()}.
	 */
	public int modified() {
		return modified.get();
	}
	
	/** Runs HyperBall. The computation will stop when {@link #modified()} returns false. */
	public void run() throws IOException {
		run( Long.MAX_VALUE );
	}
	
	/** Runs HyperBall.
	 * 
	 * @param upperBound an upper bound to the number of iterations.
	 */
	public void run( final long upperBound ) throws IOException {
		run( upperBound, -1 );
	}

	/** Runs HyperBall.
	 * 
	 * @param upperBound an upper bound to the number of iterations.
	 * @param threshold a value that will be used to stop the computation by relative increment if the neighbourhood function is being computed; if you specify -1,
	 * the computation will stop when {@link #modified()} returns false.
	 */
	public void run( long upperBound, final double threshold ) throws IOException {
		run( upperBound, threshold, seed );
	}

	/** Runs HyperBall.
	 * 
	 * @param upperBound an upper bound to the number of iterations.
	 * @param threshold a value that will be used to stop the computation by relative increment if the neighbourhood function is being computed; if you specify -1,
	 * the computation will stop when {@link #modified()} returns false.
	 * @param seed the random seed passed to {@link HyperLogLogCounterArray#HyperLogLogCounterArray(long, long, int, long)}.
	 */
	public void run( long upperBound, final double threshold, final long seed ) throws IOException {
		upperBound = Math.min( upperBound, numNodes );

		init( seed );
		
		for( long i = 0; i < upperBound; i++ ) {
			iterate();

			if ( modified() == 0 ) {
				info( "Terminating approximation after " + i + " iteration(s) by stabilisation" );
				break;
			}

			if ( i > 3 && relativeIncrement < ( 1 + threshold ) ) {
				info( "Terminating approximation after " + i + " iteration(s) by relative bound on the neighbourhood function" );
				break;
			}
		}

		if ( pl != null ) pl.done();
	}
	
	/** Throws a {@link NotSerializableException}, as this class implements {@link Serializable}
	 * because it extends {@link HyperLogLogCounterArray}, but it's not really. */
	private void writeObject( @SuppressWarnings("unused") final ObjectOutputStream oos ) throws IOException {
        throw new NotSerializableException();
    }

	
	public static void main( String arg[] ) throws IOException, JSAPException, IllegalArgumentException, ClassNotFoundException, IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException {
		SimpleJSAP jsap = new SimpleJSAP( HyperBall.class.getName(), "Runs HyperBall on the given graph, possibly computing positive geometric centralities.\n\nPlease note that to compute negative centralities on directed graphs (which is usually what you want) you have to compute positive centralities on the transpose.",
			new Parameter[] {
			new FlaggedOption( "log2m", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'l', "log2m", "The logarithm of the number of registers." ),
			new FlaggedOption( "upperBound", JSAP.LONGSIZE_PARSER, Long.toString( Long.MAX_VALUE ), JSAP.NOT_REQUIRED, 'u', "upper-bound", "An upper bound to the number of iterations." ),
			new FlaggedOption( "threshold", JSAP.DOUBLE_PARSER, "-1", JSAP.NOT_REQUIRED, 't', "threshold", "A threshold that will be used to stop the computation by relative increment. If it is -1, the iteration will stop only when all registers do not change their value (recommended)." ),
			new FlaggedOption( "threads", JSAP.INTSIZE_PARSER, "0", JSAP.NOT_REQUIRED, 'T', "threads", "The number of threads to be used. If 0, the number will be estimated automatically." ),
			new FlaggedOption( "granularity", JSAP.INTSIZE_PARSER, Integer.toString( DEFAULT_GRANULARITY ), JSAP.NOT_REQUIRED, 'g',  "granularity", "The number of node per task in a multicore environment." ),
			new FlaggedOption( "bufferSize", JSAP.INTSIZE_PARSER, Util.formatBinarySize( DEFAULT_BUFFER_SIZE ), JSAP.NOT_REQUIRED, 'b',  "buffer-size", "The size of an I/O buffer in bytes." ),
			new FlaggedOption( "neighbourhoodFunction", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'n',  "neighbourhood-function", "Store an approximation the neighbourhood function in text format." ),
			new FlaggedOption( "sumOfDistances", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'd',  "sum-of-distances", "Store an approximation of the sum of distances from each node as a binary list of floats." ),
			new FlaggedOption( "harmonicCentrality", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'h',  "harmonic-centrality", "Store an approximation of the positive harmonic centrality (the sum of the reciprocals of distances from each node) as a binary list of floats." ),
			new FlaggedOption( "discountedGainCentrality", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'z',  "discounted-gain-centrality", "A positive discounted gain centrality to be approximated and stored; it is specified as O:F where O is the spec of an object of type Int2DoubleFunction and F is the name of the file where the binary list of floats will be stored. The spec can be either the name of a public field of HyperBall, or a constructor invocation of a class implementing Int2DoubleFunction." ).setAllowMultipleDeclarations( true ),
			new FlaggedOption( "closenessCentrality", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'c',  "closeness-centrality", "Store an approximation of the positive closeness centrality of each node (the reciprocal of sum of the distances from each node) as a binary list of floats. Terminal nodes will have centrality equal to zero." ),
			new FlaggedOption( "linCentrality", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'L',  "lin-centrality", "Store an approximation of the positive Lin centrality of each node (the reciprocal of sum of the distances from each node multiplied by the square of the number of nodes reachable from the node) as a binary list of floats. Terminal nodes will have centrality equal to one." ),
			new FlaggedOption( "nieminenCentrality", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'N',  "nieminen-centrality", "Store an approximation of the positive Nieminen centrality of each node (the square of the number of nodes reachable from each node minus the sum of the distances from the node) as a binary list of floats." ),
			new FlaggedOption( "reachable", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'r',  "reachable", "Store an approximation of the number of nodes reachable from each node as a binary list of floats." ),
			new FlaggedOption( "seed", JSAP.LONG_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'S', "seed", "The random seed." ),
			new Switch( "spec", 's', "spec", "The basename is not a basename but rather a specification of the form <ImmutableGraphImplementation>(arg,arg,...)." ),
			new Switch( "offline", 'o', "offline", "Do not load the graph in main memory. If this option is used, the graph will be loaded in offline (for one thread) or mapped (for several threads) mode." ),
			new Switch( "external", 'e', "external", "Use an external dump file instead of core memory to store new counter values. Note that the file might be very large: you might need to set suitably the Java temporary directory (-Djava.io.tmpdir=DIR)." ),
			new UnflaggedOption( "basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the graph." ),
			new UnflaggedOption( "basenamet", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, JSAP.NOT_GREEDY, "The basename of the transpose graph for systolic computations (strongly suggested). If it is equal to <basename>, the graph will be assumed to be symmetric and will be loaded just once." ),
			}		
		);

		JSAPResult jsapResult = jsap.parse( arg );
		if ( jsap.messagePrinted() ) System.exit( 1 );
		
		final boolean spec = jsapResult.getBoolean( "spec" );
		final boolean external = jsapResult.getBoolean( "external" );
		final boolean offline = jsapResult.getBoolean( "offline" );
		final String neighbourhoodFunctionFile = jsapResult.getString( "neighbourhoodFunction" );
		final boolean neighbourhoodFunction = jsapResult.userSpecified( "neighbourhoodFunction" );
		final String sumOfDistancesFile = jsapResult.getString( "sumOfDistances" );
		final boolean sumOfDistances = jsapResult.userSpecified( "sumOfDistances" );
		final String harmonicCentralityFile = jsapResult.getString( "harmonicCentrality" );
		final boolean harmonicCentrality = jsapResult.userSpecified("harmonicCentrality" );
		final String closenessCentralityFile = jsapResult.getString( "closenessCentrality" );
		final boolean closenessCentrality = jsapResult.userSpecified( "closenessCentrality" );
		final String linCentralityFile = jsapResult.getString( "linCentrality" );
		final boolean linCentrality = jsapResult.userSpecified("linCentrality" );
		final String nieminenCentralityFile = jsapResult.getString( "nieminenCentrality" );
		final boolean nieminenCentrality = jsapResult.userSpecified("nieminenCentrality" );
		final String reachableFile = jsapResult.getString( "reachable" );
		final boolean reachable = jsapResult.userSpecified("reachable" );
		final String basename = jsapResult.getString( "basename" );
		final String basenamet = jsapResult.getString( "basenamet" );
		final ProgressLogger pl = new ProgressLogger( LOGGER );
		final int log2m = jsapResult.getInt( "log2m" );
		final int threads = jsapResult.getInt( "threads" );
		final int bufferSize = jsapResult.getInt( "bufferSize" );
		final int granularity = jsapResult.getInt( "granularity" );
		final long seed = jsapResult.userSpecified( "seed" ) ? jsapResult.getLong( "seed" ) : Util.randomSeed();
		
		final String[] discountedGainCentralitySpec = jsapResult.getStringArray( "discountedGainCentrality" );
		final Int2DoubleFunction[] discountFunction = new Int2DoubleFunction[ discountedGainCentralitySpec.length ];
		final String[] discountedGainCentralityFile = new String[ discountedGainCentralitySpec.length ];
		for ( int i = 0; i < discountedGainCentralitySpec.length; i++ ) {
			int pos = discountedGainCentralitySpec[ i ].indexOf( ':' );
			if ( pos < 0 ) throw new IllegalArgumentException( "Wrong spec <" + discountedGainCentralitySpec[ i ] + ">" );
			discountedGainCentralityFile[ i ] = discountedGainCentralitySpec[ i ].substring( pos + 1 );
			String gainSpec = discountedGainCentralitySpec[ i ].substring( 0, pos );
			Int2DoubleFunction candidateFunction;
			try {
				candidateFunction = (Int2DoubleFunction)HyperBall.class.getField( gainSpec ).get( null );
			}
			catch ( SecurityException e ) {
				throw new IllegalArgumentException( "Field " + gainSpec + " exists but cannot be accessed", e );
			}
			catch ( ClassCastException e ) {
				throw new IllegalArgumentException( "Field " + gainSpec + " exists but it is not of type Int2DoubleFunction", e );
			}
			catch ( NoSuchFieldException e ) {
				candidateFunction = null;
			}
			discountFunction[ i ] = candidateFunction == null? ObjectParser.fromSpec( gainSpec, Int2DoubleFunction.class ) : candidateFunction;
		}
		
		final ImmutableGraph graph = spec 
				? ObjectParser.fromSpec( basename, ImmutableGraph.class, GraphClassParser.PACKAGE ) 
				: offline	
					? ( ( numberOfThreads( threads ) == 1 && basenamet == null ? ImmutableGraph.loadOffline( basename ) : ImmutableGraph.loadMapped( basename, new ProgressLogger() ) ) ) 
					: ImmutableGraph.load( basename, new ProgressLogger() ); 

		final ImmutableGraph grapht = basenamet == null ? null : basenamet.equals( basename ) ? graph : spec ? ObjectParser.fromSpec( basenamet, ImmutableGraph.class, GraphClassParser.PACKAGE ) : 
			offline ? ImmutableGraph.loadMapped( basenamet, new ProgressLogger() ) : ImmutableGraph.load( basenamet, new ProgressLogger() ); 

		final HyperBall hyperBall = new HyperBall( graph, grapht, log2m, pl, threads, bufferSize, granularity, external, sumOfDistances || closenessCentrality || linCentrality || nieminenCentrality, harmonicCentrality, discountFunction, seed );
		hyperBall.run( jsapResult.getLong( "upperBound" ), jsapResult.getDouble( "threshold" ) );
		hyperBall.close();

		if ( neighbourhoodFunction ) {
			final PrintStream stream = new PrintStream( new FastBufferedOutputStream( new FileOutputStream( neighbourhoodFunctionFile ) ) );
			for( DoubleIterator i = hyperBall.neighbourhoodFunction.iterator(); i.hasNext(); ) stream.println( BigDecimal.valueOf( i.nextDouble() ).toPlainString() );
			stream.close();
		}
		
		if ( sumOfDistances ) BinIO.storeFloats( hyperBall.sumOfDistances, sumOfDistancesFile );
		if ( harmonicCentrality ) BinIO.storeFloats( hyperBall.sumOfInverseDistances, harmonicCentralityFile );
		for ( int i = 0; i < discountedGainCentralitySpec.length; i++ ) BinIO.storeFloats( hyperBall.discountedCentrality[ i ], discountedGainCentralityFile[ i ] );
		if ( closenessCentrality ) {
			final int n = graph.numNodes();
			final DataOutputStream dos = new DataOutputStream( new FastBufferedOutputStream( new FileOutputStream( closenessCentralityFile ) ) ); 
			for ( int i = 0; i < n; i++ ) dos.writeFloat( hyperBall.sumOfDistances[ i ] == 0 ? 0 : 1 / hyperBall.sumOfDistances[ i ] );
			dos.close();
		}
		if ( linCentrality ) {
			final int n = graph.numNodes();
			final DataOutputStream dos = new DataOutputStream( new FastBufferedOutputStream( new FileOutputStream( linCentralityFile ) ) ); 
			for ( int i = 0; i < n; i++ ) {
				// Lin's index for isolated nodes is by (our) definition one (it's smaller than any other node).
				if ( hyperBall.sumOfDistances[ i ] == 0 ) dos.writeFloat( 1 );
				else {
					final double count = hyperBall.count( i );
					dos.writeFloat( (float)( count * count / hyperBall.sumOfDistances[ i ] ) );
				}
			}
			dos.close();
		}
		if ( nieminenCentrality ) {
			final int n = graph.numNodes();
			final DataOutputStream dos = new DataOutputStream( new FastBufferedOutputStream( new FileOutputStream( nieminenCentralityFile ) ) ); 
			for ( int i = 0; i < n; i++ ) {
				final double count = hyperBall.count( i );
				dos.writeFloat( (float)( count * count - hyperBall.sumOfDistances[ i ] ) );
			}
			dos.close();
		}
		if ( reachable ) {
			final int n = graph.numNodes();
			final DataOutputStream dos = new DataOutputStream( new FastBufferedOutputStream( new FileOutputStream( reachableFile ) ) ); 
			for ( int i = 0; i < n; i++ ) dos.writeFloat( (float)hyperBall.count( i ) );
			dos.close();
		}
	}
}
