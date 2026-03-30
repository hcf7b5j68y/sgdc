package sgdc.dc.evidenceset.build.sampling;

import java.util.*;
import java.util.concurrent.*;

import sgdc.dc.algorithms.sgdc.SgdcRunner;
import sgdc.dc.evidenceset.IEvidenceSet;
import sgdc.dc.evidenceset.TroveEvidenceSet;
import sgdc.dc.evidenceset.build.EvidenceSetBuilder;
import sgdc.dc.input.ColumnPair;
import sgdc.dc.input.Input;
import sgdc.dc.predicates.Predicate;
import sgdc.dc.predicates.PredicateBuilder;
import sgdc.dc.predicates.sets.PredicateBitSet;

/**
 * AdaptiveEvidenceSetBuilderParallel
 * - 初始采样 & 自适应采样阶段全并行化
 * - 使用误差收敛 (epsilon, z) 控制停止
 */
public class RandomSampling extends EvidenceSetBuilder {

    private final double epsilon;
    private final double zValue;
    private final int initialBatch;
    private final int maxPairs;
    private final int k;
    private final int numThreads;
    private final int blockSize; // 初始采样每块行数

    public RandomSampling(PredicateBuilder pred) {
        super(pred);
        this.epsilon = 0.03d;
        this.zValue = 1.96; // 95% CI
        this.initialBatch = 2000;
        this.maxPairs = 100000;
        this.k = 5;
        this.numThreads = Runtime.getRuntime().availableProcessors()-1;
        this.blockSize = 1000; // 可根据数据规模调
    }

    public IEvidenceSet buildEvidenceSet(Input input) {
        Collection<ColumnPair> pairs = predicates.getColumnPairs();
        createSets(pairs);

        IEvidenceSet evidenceSet = new TroveEvidenceSet();
        Predicate[] predicateList = predicates.getPredicates().toArray(new Predicate[0]);
        int numPredicates = predicateList.length;

        long[] trials = new long[numPredicates];
        long[] successes = new long[numPredicates];

        int lineCount = input.getLineCount();
        int pairsSampled = 0;
        int batchSize = initialBatch;

        ExecutorService pool = Executors.newFixedThreadPool(numThreads);

        // =============================
        // ① 初始采样：并行按行块处理
        // =============================
        List<Future<ThreadResult>> initFutures = new ArrayList<>();
        for (int start = 0; start < lineCount; start += blockSize) {
            final int s = start;
            final int e = Math.min(start + blockSize, lineCount);
            initFutures.add(pool.submit(() -> {
                Random r = new Random();
                long[] localTrials = new long[numPredicates];
                long[] localSuccesses = new long[numPredicates];
                Set<PredicateBitSet> localEvidence = new HashSet<>();

                for (int i = s; i < e; i++) {
                    PredicateBitSet staticSet = getStatic(pairs, i);
                    for (int count = 0; count < k; ++count) {
                        int j = r.nextInt(lineCount - 1);
                        if (j >= i) j++;
                        PredicateBitSet set = getPredicateSet(staticSet, pairs, i, j);
                        localEvidence.add(set);
                        SgdcRunner.sampleCount.incrementAndGet();
                        for (int p = 0; p < numPredicates; p++) {
                            localTrials[p]++;
                            if (set.contains(predicateList[p])) {
                                localSuccesses[p]++;
                            }
                        }
                    }
                }
                return new ThreadResult(localEvidence, localTrials, localSuccesses);
            }));
        }

        for (Future<ThreadResult> f : initFutures) {
            try {
                ThreadResult res = f.get();
                synchronized (evidenceSet) {
                    for (PredicateBitSet pb : res.localEvidence) {
                        evidenceSet.add(pb);
                    }
                }
                for (int p = 0; p < numPredicates; p++) {
                    trials[p] += res.localTrials[p];
                    successes[p] += res.localSuccesses[p];
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        pairsSampled += lineCount * k;

        // =============================
        // ② 自适应采样：并行批次
        // =============================
        while (pairsSampled < maxPairs) {
            int remaining = Math.min(batchSize, maxPairs - pairsSampled);
            int perThread = (remaining + numThreads - 1) / numThreads;

            List<Future<ThreadResult>> futures = new ArrayList<>();
            for (int t = 0; t < numThreads; t++) {
                final int startCount = t * perThread;
                final int endCount = Math.min(remaining, (t + 1) * perThread);
                if (startCount >= endCount) continue;

                futures.add(pool.submit(() -> {
                    Random r = new Random();
                    long[] localTrials = new long[numPredicates];
                    long[] localSuccesses = new long[numPredicates];
                    Set<PredicateBitSet> localEvidence = new HashSet<>();

                    for (int c = startCount; c < endCount; c++) {
                        int i = r.nextInt(lineCount);
                        int j = r.nextInt(lineCount - 1);
                        if (j >= i) j++;

                        PredicateBitSet staticSet = getStatic(pairs, i);
                        PredicateBitSet set = getPredicateSet(staticSet, pairs, i, j);
                        localEvidence.add(set);

                        for (int p = 0; p < numPredicates; p++) {
                            localTrials[p]++;
                            if (set.contains(predicateList[p])) {
                                localSuccesses[p]++;
                            }
                        }
                    }
                    return new ThreadResult(localEvidence, localTrials, localSuccesses);
                }));
            }

            for (Future<ThreadResult> f : futures) {
                try {
                    ThreadResult res = f.get();
                    synchronized (evidenceSet) {
                        for (PredicateBitSet pb : res.localEvidence) {
                            evidenceSet.add(pb);
                        }
                    }
                    for (int p = 0; p < numPredicates; p++) {
                        trials[p] += res.localTrials[p];
                        successes[p] += res.localSuccesses[p];
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }

            pairsSampled += remaining;

            // === 收敛判断 ===
            if (hasConverged(trials, successes, epsilon, zValue)) {
                break;
            }

            // === 自适应增大 batch ===
            batchSize = Math.min(batchSize * 2, maxPairs - pairsSampled);
        }

        pool.shutdown();
        return evidenceSet;
    }

    private boolean hasConverged(long[] trials, long[] successes,
                                 double epsilon, double z) {
        for (int i = 0; i < trials.length; i++) {
            if (trials[i] == 0) return false;
            double pHat = (double) successes[i] / trials[i];
            double se = Math.sqrt(pHat * (1.0 - pHat) / trials[i]);
            double halfWidth = z * se;
            if (halfWidth > epsilon) return false;
        }
        return true;
    }

    private static class ThreadResult {
        final Set<PredicateBitSet> localEvidence;
        final long[] localTrials;
        final long[] localSuccesses;

        ThreadResult(Set<PredicateBitSet> e, long[] t, long[] s) {
            this.localEvidence = e;
            this.localTrials = t;
            this.localSuccesses = s;
        }
    }
}
