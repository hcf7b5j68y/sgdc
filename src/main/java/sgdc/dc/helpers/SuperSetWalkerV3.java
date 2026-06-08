package sgdc.dc.helpers;

import ch.javasoft.bitset.IBitSet;
import ch.javasoft.bitset.LongBitSet;
import sgdc.dc.denialcontraints.DenialConstraint;
import sgdc.dc.evidenceset.HashEvidenceSet;
import sgdc.dc.evidenceset.build.PartitionEvidenceSetBuilder;
import sgdc.dc.paritions.ClusterPair;
import sgdc.dc.paritions.IEJoin;
import sgdc.dc.predicates.PartitionRefiner;
import sgdc.dc.predicates.sets.PredicateBitSet;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import java.util.concurrent.atomic.AtomicInteger;

public class SuperSetWalkerV3 {

    private List<IBitSet> sortedList;
    private BitSetTranslator translator;

    // 新增字段，存放过滤后的组合列表
    private List<IBitSet> validCombinations;

    public SuperSetWalkerV3(Collection<IBitSet> keySet, int[] counts) {
        System.out.println("counts的大小"+counts.length);
        ArrayIndexComparator comparator = new ArrayIndexComparator(counts, ArrayIndexComparator.Order.ASCENDING);
        this.translator = new BitSetTranslator(comparator.createIndexArray());
        this.sortedList = new ArrayList<>(translator.transform(keySet));
        Collections.sort(this.sortedList, (o1, o2) -> o2.compareTo(o1)); // 按字典序降序排列
    }

    private Map<IBitSet, List<DenialConstraint>> predicateDCMap;
    private IndexProvider<PartitionRefiner> indexProvider;
    private PartitionEvidenceSetBuilder builder;
    private HashEvidenceSet resultEv;
    private ClusterPair startPartition;
    private IEJoin iejoin;
    private long startTime;

    private int max = 0;

    public void walk(
            Map<IBitSet, List<DenialConstraint>> predicateDCMap,
            IndexProvider<PartitionRefiner> indexProvider,
            PartitionEvidenceSetBuilder builder,
            HashEvidenceSet resultEv,
            ClusterPair startPartition,
            IEJoin iejoin
    ) {
        this.predicateDCMap = predicateDCMap;
        this.indexProvider = indexProvider;
        this.builder = builder;
        this.resultEv = resultEv;
        this.startPartition = startPartition;
        this.iejoin = iejoin;
        this.startTime = System.nanoTime();

        // 根据 predicateDCMap.keySet() 构造只需遍历的组合集合 validCombinations
        this.validCombinations = new ArrayList<>(translator.transform(predicateDCMap.keySet()));
        Collections.sort(this.validCombinations, (o1, o2) -> o2.compareTo(o1)); // 降序排序

        IBitSet root = LongBitSet.FACTORY.create();

        for (int i = 0; i < validCombinations.size(); i++) { // 这里用 validCombinations 替代 sortedList
            IBitSet candidate = validCombinations.get(i);
            if (root.isSubSetOf(candidate)) {
                int nextBit = candidate.nextSetBit(0);
                if (nextBit < 0) continue;

                IBitSet toCheck = root.clone();
                toCheck.set(nextBit);

                long taskStart = System.currentTimeMillis();
                AtomicInteger maxDepth = new AtomicInteger(0);
                HashEvidenceSet localEv = new HashEvidenceSet(); // 分支独立 localEv

                processNode(toCheck, nextBit, i, null, 1, maxDepth, localEv);

                long duration = System.currentTimeMillis() - taskStart;
                int localSize = localEv.size();

                for (PredicateBitSet j : localEv) {
                    this.resultEv.add(j);
                }

                System.out.printf("(bit=%d) took %d ms, maxDepth=%d, localEvSize=%d%n",
                        nextBit, duration, maxDepth.get(), localSize);

                while (i + 1 < validCombinations.size() && toCheck.isSubSetOf(validCombinations.get(i + 1))) {
                    i++;
                }
            }
        }
    }

    private void processNode(IBitSet toCheck, int nextBit, int listStart, ClusterPair parentRes,
                             int depth, AtomicInteger maxDepth, HashEvidenceSet localEv) {
        if ((System.nanoTime() - startTime) > TimeUnit.MINUTES.toNanos(120)) return;

        maxDepth.updateAndGet(prev -> Math.max(prev, depth));

        int newRefiner = translator.retransform(nextBit);
        IBitSet currentBits = translator.bitsetRetransform(toCheck);
        ClusterPair clusterPair = parentRes;

        Consumer<ClusterPair> consumer = (cluster) -> {
            List<DenialConstraint> currentDCs = predicateDCMap.get(currentBits);
            if (currentDCs != null) {
                builder.addEvidences(cluster, localEv); // 写入分支的 localEv
            } else {
                walkChildren(listStart, toCheck, cluster, nextBit, depth + 1, maxDepth, localEv);
            }
        };

        PartitionRefiner refiner = indexProvider.getObject(newRefiner);
        ClusterPair partition = clusterPair != null ? clusterPair : startPartition;

        partition.refine(refiner, iejoin, consumer);
    }

    private void walkChildren(int next, IBitSet parent, ClusterPair parentRes,
                              int lastBit, int depth, AtomicInteger maxDepth, HashEvidenceSet localEv) {
        // 用 validCombinations 替代 sortedList
        while (next < validCombinations.size() && parent.isSubSetOf(validCombinations.get(next))) {
            int nextBit = validCombinations.get(next).nextSetBit(lastBit + 1);
            if (nextBit < 0) {
                ++next;
                if (next > max) max = next;
            } else {
                IBitSet toCheck = parent.clone();
                toCheck.set(nextBit);
                final int nextF = next;

                if ((System.nanoTime() - startTime) > TimeUnit.MINUTES.toNanos(120)) return;

                Consumer<ClusterPair> consumer = (cluster) -> {
                    IBitSet currentBits = translator.bitsetRetransform(toCheck);
                    List<DenialConstraint> currentDCs = predicateDCMap.get(currentBits);
                    if (currentDCs != null) {
                        builder.addEvidences(cluster, localEv); // 写入 localEv
                    } else {
                        walkChildren(nextF, toCheck, cluster, nextBit, depth + 1, maxDepth, localEv);
                    }
                };

                maxDepth.updateAndGet(prev -> Math.max(prev, depth));

                int newRefiner = translator.retransform(nextBit);
                PartitionRefiner refiner = indexProvider.getObject(newRefiner);
                ClusterPair partition = parentRes != null ? parentRes : startPartition;

                partition.refine(refiner, iejoin, consumer);

                while (next < validCombinations.size() && toCheck.isSubSetOf(validCombinations.get(next))) {
                    ++next;
                }
                if (next > max) max = next;
            }
        }
    }
}

