package sgdc.dc.coverage;

import sgdc.dc.denialcontraints.DenialConstraint;
import sgdc.dc.input.Input;
import sgdc.dc.predicates.Predicate;
import sgdc.dc.util.Pair;

import java.util.*;

import static java.util.stream.IntStream.range;

public class CoverageHandler {

    private final Input input;
    private final List<Pair> sampledPairs;
    private final BitSet invalidatedFlags;
    private final int samplePairsCount;

    public CoverageHandler(Input input, long seed, int samplePairs) {
        this.input = input;
        this.samplePairsCount = samplePairs;
        this.sampledPairs = generateSamplePairs(seed);
        this.invalidatedFlags = new BitSet(samplePairs); // 初始化位图容量
    }

    private List<Pair> generateSamplePairs(long seed) {
        long n = input.getLineCount();
        if (n < 2 || samplePairsCount <= 0) {
            return List.of();
        }

        List<Pair> pairs = new ArrayList<>(samplePairsCount);
        Random random = new Random(seed);

        for (int i = 0; i < samplePairsCount; i++) {
            int t1, t2;
            do {
                t1 = random.nextInt((int) n);
                t2 = random.nextInt((int) n);
            } while (t1 == t2); // 避免自配对

            pairs.add(new Pair(t1, t2));
        }
        return pairs;
    }

    public double estimateCoverageParallel(DenialConstraint dc, int m) {
        if (sampledPairs.isEmpty() || m == 0) {
            return 0.0;
        }

        long violationCount = range(0, samplePairsCount).parallel()
                .filter(i -> !invalidatedFlags.get(i)) // 快速位图检查（纳秒级）
                .mapToLong(i -> {
                    Pair pair = sampledPairs.get(i);
                    int t1 = pair.first();   // 直接访问record组件
                    int t2 = pair.second();

                    // 检查该元组对是否违反DC（所有谓词为true）
                    boolean allTrue = true;
                    for (Predicate p : dc.getPredicateSet()) {
                        if (!p.satisfies(t1, t2)) {
                            allTrue = false;
                            break;
                        }
                    }

                    if (allTrue) {
                        invalidatedFlags.set(i); // ✂️ 原子标记索引（线程安全）
                        return 1L;
                    }
                    return 0L;
                })
                .sum();

        return (double) violationCount * ((double) dc.size() / m) / samplePairsCount;
    }

    public double estimateCoverageSerial(DenialConstraint dc, int m) {
        if (sampledPairs.isEmpty() || m == 0) {
            return 0.0;
        }

        long violationCount = 0;

        for (int i = 0; i < samplePairsCount; i++) {
            if (invalidatedFlags.get(i)) {
                continue;
            }

            Pair pair = sampledPairs.get(i);
            int t1 = pair.first();
            int t2 = pair.second();

            boolean allTrue = true;
            for (Predicate p : dc.getPredicateSet()) {
                if (!p.satisfies(t1, t2)) {
                    allTrue = false;
                    break;
                }
            }

            if (allTrue) {
                invalidatedFlags.set(i);
                violationCount++;
            }
        }

        return (double) violationCount * ((double) dc.size() / m) / samplePairsCount;
    }


    public int getRemainingPairsCount() {
        return samplePairsCount - invalidatedFlags.cardinality(); // BitSet快速统计
    }

    public void resetInvalidatedPairs() {
        invalidatedFlags.clear(); // O(1) 清空位图
    }
}