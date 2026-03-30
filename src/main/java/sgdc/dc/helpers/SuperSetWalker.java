package sgdc.dc.helpers;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import ch.javasoft.bitset.IBitSet;
import ch.javasoft.bitset.LongBitSet;
import com.google.common.util.concurrent.AtomicLongMap;
import sgdc.dc.paritions.ClusterPair;
import sgdc.dc.predicates.PartitionRefiner;

public class SuperSetWalker {
	private final double totalPairs;

	public static class InterResult {
		public int newRefiner;
		public IBitSet currentBits;
		public Consumer<ClusterPair> nextRefiner;
		public ClusterPair clusterPair;
		public double upperBound;
	}

	private final List<IBitSet> sortedList;
	private final BitSetTranslator translator;
	private final AtomicLongMap<PartitionRefiner> selectivityCount;
	private final IndexProvider<PartitionRefiner> indexProvider;

	// —— 并行相关 —— //
	private Executor executor;       // 外部注入的线程池
	private Phaser phaser;           // 追踪活跃任务
	private AtomicLong remainingTasks;
	private CountDownLatch countDownLatch;
	private volatile boolean parallel = false;

	public SuperSetWalker(Collection<IBitSet> keySet,
						  int[] counts,
						  AtomicLongMap<PartitionRefiner> selectivityCount,
						  IndexProvider<PartitionRefiner> indexProvider,
						  double pairCount) {
		ArrayIndexComparator comparator = new ArrayIndexComparator(counts, ArrayIndexComparator.Order.ASCENDING);
		this.translator = new BitSetTranslator(comparator.createIndexArray());
		this.selectivityCount = selectivityCount;
		this.sortedList = new ArrayList<>(translator.transform(keySet));
		this.indexProvider = indexProvider;
		this.totalPairs = pairCount;

		Collections.sort(this.sortedList, (o1, o2) -> o2.compareTo(o1));
	}

	// 维持原有单线程 API
	public void walk(Consumer<InterResult> f) {
		this.parallel = false;
		walkChildrenSync(0, LongBitSet.FACTORY.create(), null, -1, f);
	}

	// 新增：并行遍历（阻塞直到全部任务完成）
	public void walkParallel(Consumer<InterResult> f, Executor executor){
		this.parallel = true;
		this.executor = Objects.requireNonNull(executor, "executor must not be null");
		this.phaser = new Phaser(1); // 注册主线程
		this.remainingTasks = new AtomicLong(0);
		submit(0, LongBitSet.FACTORY.create(), null, -1, f); // 提交根任务
		// 等待所有子任务结束
		while (remainingTasks.get() > 0) {
			try{
				Thread.sleep(10);
			}catch (Exception ignored){}
		}
	}

	private void submit(int next, IBitSet parent, ClusterPair parentRes, int lastBit, Consumer<InterResult> f) {

		this.remainingTasks.incrementAndGet();
		executor.execute(() -> {
			try {

				walkChildrenParallel(next, parent, parentRes, lastBit, f);
			} finally {
				this.remainingTasks.decrementAndGet();
			}
		});
	}

	// —— 原始单线程逻辑（保持不变）—— //
	private void walkChildrenSync(int next, IBitSet parent, ClusterPair parentRes, int lastBit, Consumer<InterResult> f) {
		while (next < sortedList.size() && parent.isSubSetOf(sortedList.get(next))) {
			int nextBit = sortedList.get(next).nextSetBit(lastBit + 1);
			if (nextBit < 0) {
				++next;
			} else {
				IBitSet toCheck = parent.clone();
				toCheck.set(nextBit);
				final int nextF = next;

				Consumer<ClusterPair> refineFurther = (clusterPair) ->
						walkChildrenSync(nextF, toCheck, clusterPair, nextBit, f);

				InterResult inter = new InterResult();
				inter.clusterPair = parentRes;
				inter.newRefiner = translator.retransform(nextBit);
				inter.nextRefiner = refineFurther;
				inter.upperBound = calculateUpper(next, nextBit);
				inter.currentBits = translator.bitsetRetransform(toCheck);
				f.accept(inter);

				while (next < sortedList.size() && toCheck.isSubSetOf(sortedList.get(next))) ++next;
			}
		}
	}

	// —— 并行版本：唯一区别是 nextRefiner 通过线程池提交子任务 —— //
	private void walkChildrenParallel(int next, IBitSet parent, ClusterPair parentRes, int lastBit, Consumer<InterResult> f) {
		while (next < sortedList.size() && parent.isSubSetOf(sortedList.get(next))) {
			int nextBit = sortedList.get(next).nextSetBit(lastBit + 1);
			if (nextBit < 0) {
				++next;
			} else {
				IBitSet toCheck = parent.clone();
				toCheck.set(nextBit);
				final int nextF = next;

				Consumer<ClusterPair> refineFurther = (clusterPair) ->
						submit(nextF, toCheck, clusterPair, nextBit, f);

				InterResult inter = new InterResult();
				inter.clusterPair = parentRes;
				inter.newRefiner = translator.retransform(nextBit);
				inter.nextRefiner = refineFurther;
				inter.upperBound = calculateUpper(next, nextBit);
				inter.currentBits = translator.bitsetRetransform(toCheck);
				f.accept(inter);

				while (next < sortedList.size() && toCheck.isSubSetOf(sortedList.get(next))) ++next;
			}
		}
	}

	// 和你前面修过的一致：对 nextBit 之后的位做乘积上界
	private double calculateUpper(int next, int nextBit) {
		double product = 1.0d;
		IBitSet sup = sortedList.get(next);
		int pos = sup.nextSetBit(nextBit + 1);
		while (pos >= 0) {
			PartitionRefiner ref = indexProvider.getObject(translator.retransform(pos));
			long cnt = selectivityCount.get(ref);
			if (cnt <= 0L || totalPairs <= 0.0) return 0.0;
			double sel = cnt / totalPairs;
			if (sel <= 0.0) return 0.0;
			product *= sel;
			pos = sup.nextSetBit(pos + 1);
		}
		return Math.min(product, 1.0);
	}
}
