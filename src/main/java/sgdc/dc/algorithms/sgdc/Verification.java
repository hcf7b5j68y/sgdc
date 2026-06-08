package sgdc.dc.algorithms.sgdc;

import ch.javasoft.bitset.IBitSet;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.util.concurrent.AtomicLongMap;
import sgdc.dc.coverage.CoverageProvider;
import sgdc.dc.denialcontraints.DenialConstraint;
import sgdc.dc.denialcontraints.DenialConstraintSet;
import sgdc.dc.evidenceset.HashEvidenceSet;
import sgdc.dc.evidenceset.IEvidenceSet;
import sgdc.dc.evidenceset.build.PartitionEvidenceSetBuilder;
import sgdc.dc.helpers.IndexProvider;
import sgdc.dc.helpers.SuperSetWalker;
import sgdc.dc.input.Input;
import sgdc.dc.paritions.ClusterPair;
import sgdc.dc.paritions.IEJoin;
import sgdc.dc.paritions.StrippedPartition;
import sgdc.dc.predicates.PartitionRefiner;
import sgdc.dc.predicates.Predicate;
import sgdc.dc.predicates.PredicateBuilder;
import sgdc.dc.predicates.PredicatePair;
import sgdc.dc.predicates.sets.PredicateBitSet;
import sgdc.dc.util.DCUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public class Verification {

    public static final AtomicLong count = new AtomicLong();
    private static final Logger log = LoggerFactory.getLogger(Verification.class);

    // 超时/阈值可按需调整
    private static final long TIME_LIMIT_NANOS = TimeUnit.MINUTES.toNanos(120);
    private static final long UPPER_THRESHOLD = 500L;
    private static final long LOWER_THRESHOLD = 50L;
    static final long SMALL_PAIR_THRESHOLD = 1_000L;     // 小于这个就不并行
    static final int  MAX_ACTIVE_TASKS     = 10_000;     // 全局最多跑这么多挂起任务
    static final int  MAX_PARALLEL_DEPTH   = 4;
    public static AtomicLongMap<PartitionRefiner> selectivityCount;

    // 输入表与谓词构造器
    private final Input input;
    private final PredicateBuilder predicates;
    public static long evidenceCount = 0L;
    public CoverageProvider coverageProvider;
    // 将 refiner 的排序与 pair 的加权抽出来，便于和原版保持一致
    private static final BiFunction<AtomicLongMap<PartitionRefiner>,
            Function<PartitionRefiner, Integer>, Comparator<PartitionRefiner>> resultSorter = (
            selectivityCount, counts) -> (r2, r1) -> {

        long s1 = selectivityCount.get(r1);
        long s2 = selectivityCount.get(r2);

        // 越小越优先（信息增益更高）
        return Double.compare(1.0d * counts.apply(r1).intValue() / Math.max(1, s1),
                1.0d * counts.apply(r2).intValue() / Math.max(1, s2));
    };

    private static final BiFunction<Multiset<PredicatePair>,
            AtomicLongMap<PartitionRefiner>, Function<PredicatePair, Double>> pairWeight = (
            paircountDC, selectivityCount) -> (pair) ->
            Double.valueOf(1.0d * selectivityCount.get(pair) / Math.max(1, paircountDC.count(pair)));

    public Verification(Input input, PredicateBuilder predicates) {
        this.input = input;
        this.predicates = predicates;
        coverageProvider = new CoverageProvider(input, predicates, 10000);
    }

    public HashEvidenceSet verify(DenialConstraintSet set,
                                    IEvidenceSet sampleEvidence,
                                    IEvidenceSet fullEvidence,
                                  HashSet<DenialConstraint> marked) {


        Multiset<PredicatePair> paircountDC = frequencyEstimationForPredicatePairs(set);

        selectivityCount =
                createSelectivityEstimation(sampleEvidence, paircountDC.elementSet());


        long evCount = evidenceCount;
        // =========================
        ArrayList<PredicatePair> sortedPredicatePairs =
                getSortedPredicatePairs(paircountDC, selectivityCount);

        IndexProvider<PartitionRefiner> indexProvider = new IndexProvider<>();

        Map<IBitSet, List<DenialConstraint>> predicateDCMap =
                groupDCs(set, sortedPredicatePairs, indexProvider, selectivityCount);

        int[] refinerPriorities =
                getRefinerPriorities(selectivityCount, indexProvider, predicateDCMap);

        // 7) SuperSetWalker
        double totalPairs = 1.0d * evidenceCount;
        SuperSetWalker walker = new SuperSetWalker(
                predicateDCMap.keySet(),
                refinerPriorities,
                selectivityCount,
                indexProvider,
                totalPairs
        );

        // 8) 证据构建
        HashEvidenceSet resultEv = new HashEvidenceSet();
        for (PredicateBitSet ps : fullEvidence) {
            resultEv.add(ps);
        }
        ClusterPair startPartition = StrippedPartition.getFullParition(input.getLineCount());
        int[][] values = input.getInts();
        IEJoin iejoin = new IEJoin(values);
        PartitionEvidenceSetBuilder builder = new PartitionEvidenceSetBuilder(predicates, values);

        long startTime = System.nanoTime();

        int parallelism = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);
        ExecutorService pool = Executors.newWorkStealingPool(parallelism);
        AtomicInteger activeTasks = new AtomicInteger(0);
        CountDownLatch done = new CountDownLatch(1);
        IEvidenceSet delta = new HashEvidenceSet();

        walker.walk((inter) -> {
            if ((System.nanoTime() - startTime) > TIME_LIMIT_NANOS) {
                return; // 超时直接返回
            }
            // 创建一个消费者：当需要 refine 时提交任务
            PartitionRefiner refiner = indexProvider.getObject(inter.newRefiner);
            ClusterPair partition = (inter.clusterPair != null) ? inter.clusterPair : startPartition;

            Consumer<ClusterPair> consumer = (clusterPair) -> {
                List<DenialConstraint> currentDCs = predicateDCMap.get(inter.currentBits);
                if (currentDCs != null) {
                    if(clusterPair != null && clusterPair.getSize()!=0)
                        marked.addAll(currentDCs);
                    builder.addEvidences(clusterPair, resultEv);
                }
                else  {
                    inter.nextRefiner.accept(clusterPair);
                }
            };

            activeTasks.incrementAndGet();
            pool.submit(() -> {
                try {

                    partition.refine(refiner, iejoin, consumer);
                } finally {
                    if (activeTasks.decrementAndGet() == 0) {
                        done.countDown();
                    }
                }
            });
        });

        try {
            done.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            pool.shutdown();
        }

        return resultEv;
    }




    private int[] getRefinerPriorities(AtomicLongMap<PartitionRefiner> selectivityCount,
                                       IndexProvider<PartitionRefiner> indexProvider,
                                       Map<IBitSet, List<DenialConstraint>> predicateDCMap) {
        int[] counts2 = new int[indexProvider.size()];
        Arrays.fill(counts2, 1);

        for (IBitSet bitset : predicateDCMap.keySet()) {
            for (int i = bitset.nextSetBit(0); i >= 0; i = bitset.nextSetBit(i + 1)) {
                counts2[i]++;
            }
        }

        ArrayList<PartitionRefiner> refiners = new ArrayList<>();
        int[] priorityIndex = new int[indexProvider.size()];

        for (int i = 0; i < priorityIndex.length; ++i) {
            PartitionRefiner refiner = indexProvider.getObject(i);
            refiners.add(refiner);
        }

        refiners.sort(resultSorter.apply(selectivityCount,
                refiner -> Integer.valueOf(counts2[indexProvider.getIndex(refiner).intValue()])));

        int rank = 0;
        for (PartitionRefiner refiner : refiners) {
            priorityIndex[indexProvider.getIndex(refiner).intValue()] = rank++;
        }
        return priorityIndex;
    }

    private Map<IBitSet, List<DenialConstraint>> groupDCs(DenialConstraintSet set,
                                                          ArrayList<PredicatePair> sortedPredicatePairs,
                                                          IndexProvider<PartitionRefiner> indexProvider,
                                                          AtomicLongMap<PartitionRefiner> selectivityCount) {
        Map<IBitSet, List<DenialConstraint>> predicateDCMap = new HashMap<>();
        HashMap<PredicatePair, Integer> prios = new HashMap<>();
        for (int i = 0; i < sortedPredicatePairs.size(); ++i) {
            prios.put(sortedPredicatePairs.get(i), Integer.valueOf(i));
        }
        for (DenialConstraint dc : set) {
            Set<PartitionRefiner> refinerSet = getRefinerSet(prios, dc);
            predicateDCMap.computeIfAbsent(indexProvider.getBitSet(refinerSet), (k) -> new ArrayList<>()).add(dc);
        }
        return predicateDCMap;
    }

    private Set<PartitionRefiner> getRefinerSet(HashMap<PredicatePair, Integer> prios, DenialConstraint dc) {
        Set<PartitionRefiner> refinerSet = new HashSet<>();
        Set<Predicate> pairSet = new HashSet<>();

        dc.getPredicateSet().forEach(p -> {
            if (StrippedPartition.isSingleSupported(p)) {
                refinerSet.add(p);
            } else {
                pairSet.add(p);
            }
        });

        while (pairSet.size() > 1) {
            PredicatePair bestP = getBest(prios, pairSet);
            refinerSet.add(bestP);
            pairSet.remove(bestP.getP1());
            pairSet.remove(bestP.getP2());
        }
        if (!pairSet.isEmpty()) {
            refinerSet.add(pairSet.iterator().next());
        }
        return refinerSet;
    }

    private PredicatePair getBest(HashMap<PredicatePair, Integer> prios, Set<Predicate> pairSet) {
        int best = -1;
        PredicatePair bestP = null;
        for (Predicate p1 : pairSet) {
            for (Predicate p2 : pairSet) {
                if (p1 != p2) {
                    PredicatePair pair = new PredicatePair(p1, p2);
                    Integer scoreObj = prios.get(pair);
                    if (scoreObj == null) continue; // 防守：可能不在 prios 中
                    int score = scoreObj.intValue();
                    if (score > best) {
                        best = score;
                        bestP = pair;
                    }
                }
            }
        }
        return bestP;
    }

    private ArrayList<PredicatePair> getSortedPredicatePairs(Multiset<PredicatePair> paircountDC,
                                                             AtomicLongMap<PartitionRefiner> selectivityCount) {
        ArrayList<PredicatePair> sortedPredicatePairs = new ArrayList<>();
        sortedPredicatePairs.addAll(paircountDC.elementSet());
        Function<PredicatePair, Double> weightProv = pairWeight.apply(paircountDC, selectivityCount);
        sortedPredicatePairs.sort(new Comparator<PredicatePair>() {
            @Override
            public int compare(PredicatePair o1, PredicatePair o2) {
                return Double.compare(getPriority(o2), getPriority(o1));
            }
            private double getPriority(PredicatePair o) {
                return weightProv.apply(o).doubleValue();
            }
        });
        return sortedPredicatePairs;
    }

    private Multiset<PredicatePair> frequencyEstimationForPredicatePairs(DenialConstraintSet set) {
        Multiset<PredicatePair> paircountDC = HashMultiset.create();
        for (DenialConstraint dc : set) {
            dc.getPredicateSet().forEach(p1 -> {
                if (StrippedPartition.isPairSupported(p1)) {
                    dc.getPredicateSet().forEach(p2 -> {
                        if (!p1.equals(p2) && StrippedPartition.isPairSupported(p2)) {
                            paircountDC.add(new PredicatePair(p1, p2));
                        }
                    });
                }
            });
        }
        return paircountDC;
    }

    private AtomicLongMap<PartitionRefiner> createSelectivityEstimation(IEvidenceSet sampleEvidence,
                                                                        Set<PredicatePair> predicatePairs) {
        AtomicLongMap<PartitionRefiner> selectivityCount = AtomicLongMap.create();
        evidenceCount = 0L;
        for (PredicateBitSet ps : sampleEvidence) {
            int count = (int) sampleEvidence.getCount(ps);
            evidenceCount += count;
            ps.forEach(p -> selectivityCount.addAndGet(p, count));
            for (PredicatePair pair : predicatePairs) {
                if (pair.bothContainedIn(ps)) {
                    selectivityCount.addAndGet(pair, sampleEvidence.getCount(ps));
                }
            }
        }
        return selectivityCount;
    }
}
