package sgdc.dc.denialcontraints;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import sgdc.dc.coverage.CoverageProvider;
import sgdc.dc.predicates.Predicate;
import sgdc.dc.predicates.PredicateProvider;
import sgdc.dc.util.DCUtil;
import sgdc.dc.util.PruningFactors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.javasoft.bitset.IBitSet;
import ch.javasoft.bitset.search.NTreeSearch;
import sgdc.dc.predicates.sets.Closure;
import sgdc.dc.predicates.sets.PredicateBitSet;
import sgdc.dc.predicates.sets.PredicateSetFactory;

public class DenialConstraintSet implements Iterable<DenialConstraint> {

	private Set<DenialConstraint> constraints = new HashSet<>();

	public Set<DenialConstraint> getConstraints() {
		return constraints;
	}

	public boolean contains(DenialConstraint dc) {
		return constraints.contains(dc);
	}

	private static class MinimalDCCandidate {
		DenialConstraint dc;
		IBitSet bitset;

		public MinimalDCCandidate(DenialConstraint dc) {
			this.dc = dc;
			this.bitset = PredicateSetFactory.create(dc.getPredicateSet()).getBitset();
		}

		public boolean shouldReplace(MinimalDCCandidate prior) {
			if (prior == null)
				return true;

			if (dc.getPredicateCount() < prior.dc.getPredicateCount())
				return true;

			if (dc.getPredicateCount() > prior.dc.getPredicateCount())
				return false;

			return bitset.compareTo(prior.bitset) <= 0;
		}
	}
	// 在 DenialConstraintSet 内新增：
	public void minimizeParallel() {
		List<DenialConstraint> dcs = new ArrayList<>(constraints);
		warmupPredicates(dcs); // 防止 PredicateProvider 并发写
		warmupIndexes(dcs);    // 防止 IndexProvider 并发写
		minimizeParallel(Math.max(2, Runtime.getRuntime().availableProcessors()), new MinimizeMetrics());
	}

	public void minimizeParallel(int parallelism, MinimizeMetrics mm) {
		final int cpu = Math.max(1, parallelism);

		// —— 快照 & 预热，防止并发创建谓词/索引 —— //
		List<DenialConstraint> dcs = new ArrayList<>(constraints);
		warmupPredicates(dcs);
		warmupIndexes(dcs);
		mm.nInputDC = dcs.size();

		// —— 阶段A：并行构造“closure -> MinimalDCCandidate”的 Map —— //
		// 关键点：这里与原版一致，Map 的 key 就是 PredicateBitSet（闭包对象本身），
		// equals/hashCode 完全由你的类定义决定，不做“字符串规范化”，不做“按值合并”。
		final java.util.concurrent.ConcurrentHashMap<PredicateBitSet, MinimalDCCandidate> constraintsClosureMap =
				new java.util.concurrent.ConcurrentHashMap<>(Math.max(16, dcs.size() * 2));

		dcs.parallelStream().forEach(dc -> {
			long t0 = System.nanoTime();
			Closure c = new Closure(dc.getPredicateSet());
			boolean ok = c.construct();
			long t1 = System.nanoTime();
			synchronized (mm) { mm.timeBuildClosureNs += (t1 - t0); }
			if (!ok) return;

			PredicateBitSet closure = c.getClosure();
			MinimalDCCandidate candidate = new MinimalDCCandidate(dc);

			// 与原版一致的“shouldReplace”选择：只在“相同 key（按 PredicateBitSet 的 equals/hashCode）”时发生替换。
			constraintsClosureMap.compute(closure, (k, prior) -> {
				if (candidate.shouldReplace(prior)) return candidate;
				return prior;
			});

			synchronized (mm) { mm.nKeptAfterClosure++; }
		});

		// —— 阶段B：排序（与原版一致） —— //
		List<Map.Entry<PredicateBitSet, MinimalDCCandidate>> constraints2 =
				new ArrayList<>(constraintsClosureMap.entrySet());

		long tSort0 = System.nanoTime();
		constraints2.sort((entry1, entry2) -> {
			int res = Integer.compare(entry1.getKey().size(), entry2.getKey().size());
			if (res != 0) return res;
			res = Integer.compare(entry1.getValue().dc.getPredicateCount(), entry2.getValue().dc.getPredicateCount());
			if (res != 0) return res;
			return entry1.getValue().bitset.compareTo(entry2.getValue().bitset);
		});
		long tSort1 = System.nanoTime();
		mm.timeSortNs += (tSort1 - tSort0);

		// —— 阶段C：最终 NTreeSearch 过滤（完全照抄原串行逻辑） —— //
		long tTrie0 = System.nanoTime();
		constraints = new HashSet<>();
		NTreeSearch tree = new NTreeSearch();

		for (Map.Entry<PredicateBitSet, MinimalDCCandidate> entry : constraints2) {
			// 1) 先看当前候选的闭包是否已被覆盖
			mm.trieContainsCalls++;
			if (tree.containsSubset(PredicateSetFactory.create(entry.getKey()).getBitset())) {
				continue;
			}

			// 2) 处理 inv：构造 closure 并检查是否被覆盖（注意：与原版一致，这里临时构造，不做任何提前缓存）
			DenialConstraint inv = entry.getValue().dc.getInvT1T2DC();
			if (inv != null) {
				mm.nInvTried++;
				long t0 = System.nanoTime();
				Closure c = new Closure(inv.getPredicateSet());
				boolean okInv = c.construct();
				long t1 = System.nanoTime();
				mm.timeBuildInvNs += (t1 - t0);

				if (!okInv) {
					// 原版是 continue；保持一致
					continue;
				}

				mm.trieContainsCalls++;
				if (tree.containsSubset(PredicateSetFactory.create(c.getClosure()).getBitset())) {
					continue;
				}
				mm.nInvKept++;
			}

			// 3) 接受该候选，加入原始 bitset（与原版一致：不是 closure）
			constraints.add(entry.getValue().dc);
			tree.add(entry.getValue().bitset);
			mm.trieAdds++;

			// 4) 若 inv 存在：加入 inv 的“原始谓词集 bitset”（与原版一致）
			if (inv != null) {
				tree.add(PredicateSetFactory.create(inv.getPredicateSet()).getBitset());
				mm.trieAdds++;
			}
		}
		long tTrie1 = System.nanoTime();
		mm.timeFinalTrieNs += (tTrie1 - tTrie0);

		// 日志
		mm.log(log);
	}


	public void minimize() {
		Map<PredicateBitSet, MinimalDCCandidate> constraintsClosureMap = new HashMap<>();
		for (DenialConstraint dc : constraints) {
			PredicateBitSet predicateSet = dc.getPredicateSet();
			Closure c = new Closure(predicateSet);
			if (c.construct()) {
				MinimalDCCandidate candidate = new MinimalDCCandidate(dc);
				PredicateBitSet closure = c.getClosure();
				MinimalDCCandidate prior = constraintsClosureMap.get(closure);
				if (candidate.shouldReplace(prior))
					constraintsClosureMap.put(closure, candidate);
			}
		}

		List<Entry<PredicateBitSet, MinimalDCCandidate>> constraints2 = new ArrayList<>(constraintsClosureMap.entrySet());
//		log.info("Sym size created " + constraints2.size());

		constraints2.sort((entry1, entry2) -> {
			int res = Integer.compare(entry1.getKey().size(), entry2.getKey().size());
			if (res != 0)
				return res;
			res = Integer.compare(entry1.getValue().dc.getPredicateCount(), entry2.getValue().dc.getPredicateCount());
			if (res != 0)
				return res;
			return entry1.getValue().bitset.compareTo(entry2.getValue().bitset);
		});

		constraints = new HashSet<>();
		NTreeSearch tree = new NTreeSearch();
		for (Entry<PredicateBitSet, MinimalDCCandidate> entry : constraints2) {
			if (tree.containsSubset(PredicateSetFactory.create(entry.getKey()).getBitset()))
				continue;

			DenialConstraint inv = entry.getValue().dc.getInvT1T2DC();
			if (inv != null) {
				Closure c = new Closure(inv.getPredicateSet());
				if (!c.construct())
					continue;
				if
				(tree.containsSubset(PredicateSetFactory.create(c.getClosure()).getBitset()))
				 continue;
			}

			constraints.add(entry.getValue().dc);
			tree.add(entry.getValue().bitset);
			 if(inv != null)
				 tree.add(PredicateSetFactory.create(inv.getPredicateSet()).getBitset());
		}
		// etmMonitor.render(new SimpleTextRenderer());
	}

	public void add(DenialConstraint dc) {
		constraints.add(dc);
	}

	public void addAll(DenialConstraintSet set) {
		constraints.addAll(set.constraints);
	}
	@Override
	public Iterator<DenialConstraint> iterator() {
		return constraints.iterator();
	}

	public int size() {
		return constraints.size();
	}

	public List<DenialConstraint> getConstraintsSortedByPredicateCount() {
		return constraints.stream()
				.sorted(Comparator.comparingInt(DenialConstraint::getPredicateCount))
				.collect(Collectors.toList());
	}

	public static List<DenialConstraint> getCommonDenialConstraints(
			List<DenialConstraint> list1,
			List<DenialConstraint> list2) {

		Set<DenialConstraint> set1 = new HashSet<>(list1);
		Set<DenialConstraint> set2 = new HashSet<>(list2);

		set1.retainAll(set2); // 求交集

		return new ArrayList<>(set1);
	}




	public static List<DenialConstraint> getUniqueDenialConstraints(
			List<DenialConstraint> list1,
			List<DenialConstraint> list2) {

		Set<DenialConstraint> set1 = new HashSet<>(list1);
		Set<DenialConstraint> set2 = new HashSet<>(list2);

		set1.removeAll(set2); // 求差集：list1 中有但 list2 中没有的 DC

		return new ArrayList<>(set1);
	}

	public static void sortDenialConstraints(List<DenialConstraint> list) {
		Collections.sort(list, new Comparator<DenialConstraint>() {
			@Override
			public int compare(DenialConstraint dc1, DenialConstraint dc2) {
				// 先按谓词数量升序排序
				int countCompare = Integer.compare(dc1.getPredicateCount(), dc2.getPredicateCount());
				if (countCompare != 0) {
					return countCompare;
				}

				// 如果谓词数量相同，再比较 bitset 的字典序
				IBitSet bs1 = PredicateSetFactory.create(dc1.getPredicateSet()).getBitset();
				IBitSet bs2 = PredicateSetFactory.create(dc2.getPredicateSet()).getBitset();
				return bs1.compareTo(bs2);
			}
		});

	}
	public DenialConstraintSet selectTopPPercentByPredicateCount(double p) {
		if (p <= 0 || p > 100) {
			throw new IllegalArgumentException("p must be in (0, 100]");
		}

		// 拷贝原始 DC 集合为列表
		List<DenialConstraint> list = new ArrayList<>();
		for (DenialConstraint dc : this) {
			list.add(dc);
		}

		// 按谓词数量升序排序
		Collections.sort(list, new Comparator<DenialConstraint>() {
			@Override
			public int compare(DenialConstraint dc1, DenialConstraint dc2) {
				return Integer.compare(dc1.getPredicateCount(), dc2.getPredicateCount());
			}
		});

		// 计算前 p% 的个数
		int total = list.size();
		int selectCount = (int) Math.ceil(total * p / 100.0);

		// 取出前 p% 的 DC
		DenialConstraintSet resultSet = new DenialConstraintSet();
		for (int i = 0; i < selectCount && i < total; i++) {
			resultSet.add(list.get(i));
		}

		return resultSet;
	}

	public DenialConstraintSet selectByPredicateCountThreshold(int l) {
		if (l < 0) {
			throw new IllegalArgumentException("Predicate count threshold must be >= 0");
		}

		DenialConstraintSet resultSet = new DenialConstraintSet();
		for (DenialConstraint dc : this) {
			if (dc.getPredicateCount() <= l) {
				resultSet.add(dc);
			}
		}

		return resultSet;
	}


	public static void printDenialConstraintStatsByPredicateCount(List<DenialConstraint> dcList) {
		// 使用 Map<谓词数量, 个数> 进行统计
		Map<Integer, Long> stats = dcList.stream()
				.collect(Collectors.groupingBy(
						DenialConstraint::getPredicateCount,
						TreeMap::new, // 让输出有序
						Collectors.counting()
				));

		// 打印统计结果
		System.out.println("按谓词数量分组的 DenialConstraint 个数如下：");
		for (Map.Entry<Integer, Long> entry : stats.entrySet()) {
			System.out.println("谓词数量为 " + entry.getKey() + " 的 DC 有 " + entry.getValue() + " 个");
		}
	}

	public DenialConstraintSet selectByScoreAndRange(double minP, double maxP) {
		if (minP < 0 || maxP > 100 || minP > maxP) {
			throw new IllegalArgumentException("minP and maxP must satisfy 0 <= minP <= maxP <= 100");
		}

		// 拷贝原始 DC 集合为列表
		List<DenialConstraint> list = new ArrayList<>();
		for (DenialConstraint dc : this) {
			list.add(dc);
		}

		// 按谓词数量升序排序
		list.sort(Comparator.comparingInt(DenialConstraint::getPredicateCount));

		int total = list.size();
		int startIndex = (int) Math.floor(total * minP / 100.0);
		int endIndex = (int) Math.ceil(total * maxP / 100.0);

		// 防止越界
		startIndex = Math.min(startIndex, total);
		endIndex = Math.min(endIndex, total);

		// 提取[minP%, maxP%)区间的 DC
		DenialConstraintSet resultSet = new DenialConstraintSet();
		for (int i = startIndex; i < endIndex; i++) {
			resultSet.add(list.get(i));
		}

		return resultSet;
	}

	public static DenialConstraintSet fromCollection(Collection<DenialConstraint> dcList) {
		DenialConstraintSet set = new DenialConstraintSet();
		if (dcList != null) {
			set.constraints.addAll(dcList);
		}
		return set;
	}

	public static DenialConstraintSet intersection(DenialConstraintSet set1, DenialConstraintSet set2) {
		Set<DenialConstraint> constraints1 = new HashSet<>();
		for (DenialConstraint dc : set1) {
			constraints1.add(dc);
		}

		Set<DenialConstraint> constraints2 = new HashSet<>();
		for (DenialConstraint dc : set2) {
			constraints2.add(dc);
		}

		constraints1.retainAll(constraints2); // 求交集

		DenialConstraintSet resultSet = new DenialConstraintSet();
		for (DenialConstraint dc : constraints1) {
			resultSet.add(dc);
		}

		return resultSet;
	}
	public static DenialConstraintSet difference(DenialConstraintSet setA, DenialConstraintSet setB) {
		List<DenialConstraint> listA = new ArrayList<>();
		for (DenialConstraint dc : setA) {
			listA.add(dc);
		}

		List<DenialConstraint> listB = new ArrayList<>();
		for (DenialConstraint dc : setB) {
			listB.add(dc);
		}

		List<DenialConstraint> diff = getUniqueDenialConstraints(listA, listB); // A - B

		return fromCollection(diff);
	}



	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DenialConstraintSet other = (DenialConstraintSet) obj;
		return constraints.equals(other.constraints);
	}

	private int truePositive(DenialConstraintSet prediction) {
		if (prediction == null) throw new IllegalArgumentException("prediction == null");
		Set<DenialConstraint> inter = new HashSet<>(this.constraints);
		inter.retainAll(prediction.constraints);
		return inter.size();
	}

	/** Precision(GT, Pred) = TP / |Pred| */
	public double precision(DenialConstraintSet prediction) {
		if (prediction == null) throw new IllegalArgumentException("prediction == null");
		int predSize = prediction.constraints.size();
		int tp = truePositive(prediction);
		if (predSize == 0) {
			// 若两者都空视为完全一致 => 1.0；否则 0.0
			return this.constraints.isEmpty() ? 1.0 : 0.0;
		}
		return tp / (double) predSize;
	}

	/** Recall(GT, Pred) = TP / |GT| */
	public double recall(DenialConstraintSet prediction) {
		if (prediction == null) throw new IllegalArgumentException("prediction == null");
		int gtSize = this.constraints.size();
		int tp = truePositive(prediction);
		if (gtSize == 0) {
			// 没有真实正例，通常定义为 1.0
			return 1.0;
		}
		return tp / (double) gtSize;
	}

	// 1) 预热谓词（触发创建 正向/逆向 predicate）
	static void warmupPredicates(Collection<DenialConstraint> dcs) {
		PredicateProvider pp = PredicateProvider.getInstance();

		// 收集所有 predicate
		java.util.HashSet<Predicate> all = new java.util.HashSet<>();
		for (DenialConstraint dc : dcs) {
			for (Predicate p : dc.getPredicateSet()) {
				all.add(p);
			}
		}
		// 触发正向/逆向创建
		for (Predicate p : all) {
			pp.getPredicate(p.getOperator(), p.getOperand1(), p.getOperand2());
			Predicate inv = p.getInverse();
			pp.getPredicate(inv.getOperator(), inv.getOperand1(), inv.getOperand2());
		}
	}

	// 2) 预热索引（触发为所有 predicate 分配 index 并写入反向表）
	static void warmupIndexes(Collection<DenialConstraint> dcs) {
		// 遍历所有将用到的 PredicateSet，强制调用 PredicateSetFactory.create(...)
		// 让 IndexProvider 为每个 predicate 都先分好 index
		java.util.HashSet<Predicate> all = new java.util.HashSet<>();
		for (DenialConstraint dc : dcs) {
			for (Predicate p : dc.getPredicateSet()) {
				all.add(p);
				all.add(p.getInverse());
			}
		}
		// 构造一个虚拟的 PredicateSet，把全部谓词喂给工厂（或逐个 create）
		// —— 如果你没有“合并构造”的API，就逐个触发：
		for (Predicate p : all) {
			// 这一步里通常会调用 IndexProvider.getIndex(p) 并把它放进反向结构
			PredicateSetFactory.create(p);
		}
	}

	public List<DenialConstraint> sort(Map<DenialConstraint, Double> coverageMap) {
		List<DenialConstraint> list = new ArrayList<>();
		list.addAll(getConstraints());
		list.sort(new Comparator<DenialConstraint>() {
			@Override
			public int compare(DenialConstraint dc1, DenialConstraint dc2) {
				double coverage1 = coverageMap.get(dc1);
				double coverage2 = coverageMap.get(dc2);
				double score1 = DCUtil.getScoreWithCoverage(dc1, coverage1);
				double score2 = DCUtil.getScoreWithCoverage(dc2, coverage2);
				return Double.compare(score2, score1);
			}
		});
		return list;
	}

	public DenialConstraintSet pruneWithCoverage(PruningFactors pruningFactors,
                                                 Map<DenialConstraint, Double> coverageMap){
		Set<DenialConstraint> result = new HashSet<>();
		for (DenialConstraint dc : this) {
			Double coverage = coverageMap.get(dc);
			if (DCUtil.getScoreWithCoverage(dc, coverage) > pruningFactors.threshold) {
				result.add(dc);
			}
		}

		DenialConstraintSet ret = new DenialConstraintSet();
		ret.constraints.addAll(result);
		return ret;
	}

	public DenialConstraintSet pruneWithProvider(PruningFactors pruningFactors,
												  CoverageProvider coverageProvider){
		Set<DenialConstraint> result = new HashSet<>();
		for (DenialConstraint dc : this) {
			double coverage = coverageProvider.calculate(dc);
			if (DCUtil.getScoreWithCoverage(dc, coverage) > pruningFactors.threshold) {
				result.add(dc);
			}
		}

		DenialConstraintSet ret = new DenialConstraintSet();
		ret.constraints.addAll(result);
		return ret;
	}

	private static Logger log = LoggerFactory.getLogger(DenialConstraintSet.class);
}
