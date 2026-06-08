package sgdc.dc.evidenceset.build.sampling;

import sgdc.dc.evidenceset.IEvidenceSet;
import sgdc.dc.evidenceset.build.EvidenceSetBuilder;
import sgdc.dc.input.ColumnPair;
import sgdc.dc.input.Input;
import sgdc.dc.input.ParsedColumn;
import sgdc.dc.predicates.PredicateBuilder;
import sgdc.dc.predicates.sets.PredicateBitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class FocusedSampling extends EvidenceSetBuilder {
    private enum SamplingType { WITHIN, LATER, BEFORE, OTHER; }

    // ========================= Attempts Budget =========================
    private static final class AttemptBudget {
        private final long max;
        private final AtomicLong used = new AtomicLong(0);

        AttemptBudget(long max) {
            this.max = max <= 0 ? 0 : max;
        }

        boolean tryAcquire() {
            while (true) {
                long cur = used.get();
                if (cur >= max) return false;
                if (used.compareAndSet(cur, cur + 1)) return true;
            }
        }

        boolean exhausted() { return used.get() >= max; }
        long used() { return used.get(); }
        long max() { return max; }
    }

    // ========================= Dedup proxy (ring hash) =========================
    private static final class IntHashSet {
        private static final byte EMPTY = 0;
        private static final byte FULL  = 1;
        private static final byte DEL   = 2;

        private int[] keys;
        private byte[] states;
        private int size;
        private int mask;
        private int maxFill;

        IntHashSet(int expectedSize) {
            int cap = 1;
            while (cap < expectedSize * 2) cap <<= 1;
            keys = new int[cap];
            states = new byte[cap];
            mask = cap - 1;
            maxFill = (int) (cap * 0.7);
        }

        private static int mix(int x) {
            x ^= (x >>> 16);
            x *= 0x7feb352d;
            x ^= (x >>> 15);
            x *= 0x846ca68b;
            x ^= (x >>> 16);
            return x;
        }

        boolean contains(int k) {
            int pos = mix(k) & mask;
            while (true) {
                byte st = states[pos];
                if (st == EMPTY) return false;
                if (st == FULL && keys[pos] == k) return true;
                pos = (pos + 1) & mask;
            }
        }

        void add(int k) {
            if (size >= maxFill) rehash(keys.length << 1);
            int pos = mix(k) & mask;
            int firstDel = -1;
            while (true) {
                byte st = states[pos];
                if (st == EMPTY) {
                    if (firstDel != -1) pos = firstDel;
                    states[pos] = FULL;
                    keys[pos] = k;
                    size++;
                    return;
                }
                if (st == DEL && firstDel == -1) firstDel = pos;
                else if (st == FULL && keys[pos] == k) return;
                pos = (pos + 1) & mask;
            }
        }

        void remove(int k) {
            int pos = mix(k) & mask;
            while (true) {
                byte st = states[pos];
                if (st == EMPTY) return;
                if (st == FULL && keys[pos] == k) {
                    states[pos] = DEL;
                    size--;
                    return;
                }
                pos = (pos + 1) & mask;
            }
        }

        private void rehash(int newCap) {
            int[] oldK = keys;
            byte[] oldS = states;

            keys = new int[newCap];
            states = new byte[newCap];
            mask = newCap - 1;
            maxFill = (int) (newCap * 0.7);
            size = 0;

            for (int i = 0; i < oldK.length; i++) {
                if (oldS[i] == FULL) add(oldK[i]);
            }
        }
    }

    private static final class IntRingDeduper {
        private final int cap;
        private final int[] ring;
        private int pos = 0;
        private int filled = 0;
        private final IntHashSet set;

        IntRingDeduper(int cap) {
            this.cap = Math.max(128, cap);
            this.ring = new int[this.cap];
            this.set = new IntHashSet(this.cap);
        }

        boolean isNew(int h) {
            if (set.contains(h)) return false;

            if (filled == cap) {
                int ev = ring[pos];
                set.remove(ev);
            } else {
                filled++;
            }

            ring[pos] = h;
            pos = (pos + 1) % cap;
            set.add(h);
            return true;
        }
    }

    // ========================= Distance picker (comparable only) =========================
    private static final class DistancePicker {
        private final int n;
        private final int nearMax;
        private final double alphaFar;
        private final int[] clusterW;
        private final int candidateTries;
        private final Random rnd = new Random();

        private final long[] nearPrefix;
        private final long[] farPrefix;
        private final long[] farW;

        // stats
        long nearPick = 0;
        long farPick  = 0;

        // ===== dynamic pNear =====
        private double pNear;               // mutable
        private final boolean dynamicPNear;
        private final double pNearMin, pNearMax;
        private final double pNearEta;
        private final long pNearWarmupAttempts;
        private final int pNearMinSamples;

        private long seenCrossAttempts = 0;
        private double emaNear = 0.0;
        private double emaFar  = 0.0;

        private static double clamp(double v, double lo, double hi) {
            return Math.max(lo, Math.min(hi, v));
        }

        double getPNear() { return pNear; }

        DistancePicker(
                int n,
                int nearMax,
                double pNearInit,
                double alphaFar,
                int[] clusterW,
                int candidateTries,
                boolean dynamicPNear,
                double pNearMin,
                double pNearMax,
                double pNearEta,
                long pNearWarmupAttempts,
                int pNearMinSamples
        ) {
            this.n = n;
            this.nearMax = Math.max(1, nearMax);
            this.pNear = clamp(pNearInit, 0.05, 0.95);
            this.alphaFar = alphaFar;
            this.clusterW = clusterW;
            this.candidateTries = Math.max(1, candidateTries);

            this.dynamicPNear = dynamicPNear;
            this.pNearMin = clamp(pNearMin, 0.0, 1.0);
            this.pNearMax = clamp(pNearMax, 0.0, 1.0);
            this.pNearEta = pNearEta;
            this.pNearWarmupAttempts = Math.max(0, pNearWarmupAttempts);
            this.pNearMinSamples = Math.max(1, pNearMinSamples);

            int maxD = Math.max(1, n - 1);

            this.nearPrefix = new long[Math.max(this.nearMax, 2) + 1];
            long sumN = 0;
            for (int d = 1; d <= this.nearMax; d++) {
                long w = (long) (this.nearMax + 1 - d);
                if (w <= 0) w = 1;
                sumN += w;
                nearPrefix[d] = sumN;
            }

            this.farW = new long[maxD + 1];
            this.farPrefix = new long[maxD + 1];
            long sumF = 0;
            for (int d = 1; d <= maxD; d++) {
                long w = 0;
                if (d > this.nearMax) {
                    w = (long) Math.ceil(Math.pow(d, this.alphaFar) * 1000.0);
                    if (w <= 0) w = 1;
                }
                farW[d] = w;
                sumF += w;
                farPrefix[d] = sumF;
            }
        }

        void updatePNear(long nearAttempts, long nearProxyNew, long farAttempts, long farProxyNew) {
            long a = nearAttempts + farAttempts;
            if (a <= 0) return;

            seenCrossAttempts += a;

            // not active yet
            if (!dynamicPNear) return;
            if (seenCrossAttempts < pNearWarmupAttempts) return;

            // need enough samples on both sides to compare
            if (nearAttempts < pNearMinSamples || farAttempts < pNearMinSamples) return;

            double nearRate = (double) nearProxyNew / Math.max(1L, nearAttempts);
            double farRate  = (double) farProxyNew  / Math.max(1L, farAttempts);

            // smooth
            emaNear = 0.8 * emaNear + 0.2 * nearRate;
            emaFar  = 0.8 * emaFar  + 0.2 * farRate;

            double diff = emaNear - emaFar;              // >0 => prefer near, <0 => prefer far
            double next = pNear + pNearEta * diff;

            // clamp + keep in [pNearMin, pNearMax] and also avoid degenerate 0/1
            pNear = clamp(next, Math.max(0.01, pNearMin), Math.min(0.99, pNearMax));
        }

        private int sampleNearDelta(int maxDelta) {
            int upper = Math.min(maxDelta, nearMax);
            if (upper <= 0) return 0;
            long total = nearPrefix[upper];
            long r = 1 + (Math.abs(rnd.nextLong()) % total);

            int lo = 1, hi = upper;
            while (lo < hi) {
                int mid = (lo + hi) >>> 1;
                if (nearPrefix[mid] >= r) hi = mid;
                else lo = mid + 1;
            }
            return lo;
        }

        private int sampleFarDelta(int maxDelta) {
            if (maxDelta <= nearMax) return 0;
            long total = farPrefix[maxDelta] - farPrefix[nearMax];
            if (total <= 0) return 0;
            long r = 1 + (Math.abs(rnd.nextLong()) % total);

            int lo = nearMax + 1, hi = maxDelta;
            while (lo < hi) {
                int mid = (lo + hi) >>> 1;
                long v = farPrefix[mid] - farPrefix[nearMax];
                if (v >= r) hi = mid;
                else lo = mid + 1;
            }
            return lo;
        }

        private int sampleDeltaMixedSigned(int maxDelta) {
            if (maxDelta <= 0) return 0;
            if (maxDelta <= nearMax) return -sampleNearDelta(maxDelta);

            boolean chooseNear = rnd.nextDouble() < pNear;
            if (chooseNear) return -sampleNearDelta(maxDelta);

            int dFar = sampleFarDelta(maxDelta);
            if (dFar <= 0) return -sampleNearDelta(maxDelta);
            return dFar;
        }

        private static int pack(int idx, boolean near) {
            // packed = (idx<<1) | (near?1:0)
            return (idx << 1) | (near ? 1 : 0);
        }
        private static int unpackIdx(int packed) { return packed >>> 1; }
        private static boolean unpackNear(int packed) { return (packed & 1) == 1; }

        private static final class PickRes {
            int j = -1;
            boolean near = true;
            double score = -1;
        }

        int pickBeforePacked(int i) {
            int maxDelta = i;
            if (maxDelta <= 0) return -1;

            PickRes best = new PickRes();
            for (int t = 0; t < candidateTries; t++) {
                int sd = sampleDeltaMixedSigned(maxDelta);
                if (sd == 0) continue;
                boolean near = sd < 0;
                int d = Math.abs(sd);
                int j = i - d;
                if (j < 0) continue;

                double distW = near ? (nearMax + 1 - d) : (double) farW[d];
                double score = distW * (double) clusterW[j];
                if (score > best.score) {
                    best.score = score;
                    best.j = j;
                    best.near = near;
                }
            }
            if (best.j >= 0) {
                if (best.near) nearPick++;
                else farPick++;
                return pack(best.j, best.near);
            }
            return -1;
        }

        int pickLaterPacked(int i) {
            int maxDelta = (n - 1) - i;
            if (maxDelta <= 0) return -1;

            PickRes best = new PickRes();
            for (int t = 0; t < candidateTries; t++) {
                int sd = sampleDeltaMixedSigned(maxDelta);
                if (sd == 0) continue;
                boolean near = sd < 0;
                int d = Math.abs(sd);
                int j = i + d;
                if (j >= n) continue;

                double distW = near ? (nearMax + 1 - d) : (double) farW[d];
                double score = distW * (double) clusterW[j];
                if (score > best.score) {
                    best.score = score;
                    best.j = j;
                    best.near = near;
                }
            }
            if (best.j >= 0) {
                if (best.near) nearPick++;
                else farPick++;
                return pack(best.j, best.near);
            }
            return -1;
        }

        int pickOtherPacked(int i) {
            if (n <= 1) return -1;
            boolean hasBefore = i > 0;
            boolean hasLater = i < n - 1;

            if (hasBefore && hasLater) {
                int left = i;
                int right = (n - 1) - i;
                boolean pickLeft = rnd.nextInt(left + right) < left;
                return pickLeft ? pickBeforePacked(i) : pickLaterPacked(i);
            }
            if (hasBefore) return pickBeforePacked(i);
            return pickLaterPacked(i);
        }

        static boolean packedIsNear(int packed) { return unpackNear(packed); }
        static int packedIdx(int packed) { return unpackIdx(packed); }
    }

    // ========================= Column data =========================
    private static final class ColumnData {
        final String name;
        final boolean comparable;
        final List<OrderedCluster> clusters;
        final DistancePicker dp; // comparable-only
        final WeightedRandomPicker<Integer> unorderedImportance; // unordered-only, stores clusterIndex

        long attempts = 0;
        long proxyNew = 0;

        // unordered picks count
        long unorderedOtherPick = 0;

        ColumnData(String name, boolean comparable, List<OrderedCluster> clusters,
                   DistancePicker dp, WeightedRandomPicker<Integer> unorderedImportance) {
            this.name = name;
            this.comparable = comparable;
            this.clusters = clusters;
            this.dp = dp;
            this.unorderedImportance = unorderedImportance;
        }

        int randomLineFromCluster(int idx) {
            return clusters.get(idx).nextLine();
        }

        Integer pickUnorderedOtherCluster(int withoutIdx) {
            if (unorderedImportance == null) return null;
            Integer j = unorderedImportance.getRandom(withoutIdx);
            if (j != null) unorderedOtherPick++;
            return j;
        }
    }

    // ========================= Sampling method (chunked) =========================
    private class SamplingMethod {
        final ColumnData data;
        final SamplingType type;

        double efficiency = 0.0;
        double emaGain = 0.0;

        long attempts = 0;
        long proxyNew = 0;

        int clusterCursor = 0;

        final IntRingDeduper deduper = new IntRingDeduper(4096);

        SamplingMethod(ColumnData data, SamplingType type) {
            this.data = data;
            this.type = type;
        }

        void executeChunk(Input input, IEvidenceSet evidenceSet, AttemptBudget budget, long chunkAttempts) {
            if (budget.exhausted() || chunkAttempts <= 0) return;

            int sizePrior = evidenceSet.size();

            long localAttempts = 0;
            long localProxyNew = 0;

            // for dynamic pNear (comparable cross only)
            long localNearAttempts = 0, localNearProxyNew = 0;
            long localFarAttempts  = 0, localFarProxyNew  = 0;

            int nClusters = data.clusters.size();
            if (nClusters == 0) return;

            long safetySkips = 0;
            long maxSkips = chunkAttempts * 20L;

            while (!budget.exhausted() && localAttempts < chunkAttempts) {
                if (safetySkips > maxSkips) break;

                int cIdx = clusterCursor;
                clusterCursor++;
                if (clusterCursor >= nClusters) clusterCursor = 0;

                int line1 = data.randomLineFromCluster(cIdx);
                PredicateBitSet staticSet = getStatic(pairs, line1);

                Integer otherClusterIdx = null;
                boolean isNearPick = true; // meaningful only for comparable cross

                switch (type) {
                    case WITHIN -> {
                        if (data.clusters.get(cIdx).size() <= 1) {
                            safetySkips++;
                            continue;
                        }
                        otherClusterIdx = cIdx;
                    }
                    case BEFORE -> {
                        if (!data.comparable || data.dp == null || cIdx <= 0) {
                            safetySkips++;
                            continue;
                        }
                        int packed = data.dp.pickBeforePacked(cIdx);
                        if (packed < 0) { safetySkips++; continue; }
                        otherClusterIdx = DistancePicker.packedIdx(packed);
                        isNearPick = DistancePicker.packedIsNear(packed);
                    }
                    case LATER -> {
                        if (!data.comparable || data.dp == null || cIdx >= nClusters - 1) {
                            safetySkips++;
                            continue;
                        }
                        int packed = data.dp.pickLaterPacked(cIdx);
                        if (packed < 0) { safetySkips++; continue; }
                        otherClusterIdx = DistancePicker.packedIdx(packed);
                        isNearPick = DistancePicker.packedIsNear(packed);
                    }
                    case OTHER -> {
                        if (nClusters <= 1) { safetySkips++; continue; }
                        if (data.comparable && data.dp != null) {
                            int packed = data.dp.pickOtherPacked(cIdx);
                            if (packed < 0) { safetySkips++; continue; }
                            otherClusterIdx = DistancePicker.packedIdx(packed);
                            isNearPick = DistancePicker.packedIsNear(packed);
                        } else {
                            Integer j = data.pickUnorderedOtherCluster(cIdx);
                            if (j == null) { safetySkips++; continue; }
                            otherClusterIdx = j;
                        }
                    }
                }

                int line2;
                if (otherClusterIdx == null) { safetySkips++; continue; }

                if (type == SamplingType.WITHIN) {
                    int tries = 3;
                    line2 = data.randomLineFromCluster(otherClusterIdx);
                    while (tries-- > 0 && line2 == line1) {
                        line2 = data.randomLineFromCluster(otherClusterIdx);
                    }
                    if (line2 == line1) { safetySkips++; continue; }
                } else {
                    line2 = data.randomLineFromCluster(otherClusterIdx);
                    if (line2 == line1) { safetySkips++; continue; }
                }

                if (!budget.tryAcquire()) break;

                PredicateBitSet ps = getPredicateSet(staticSet, pairs, line1, line2);
                evidenceSet.add(ps);

                localAttempts++;

                boolean isNew = deduper.isNew(ps.hashCode());
                if (isNew) localProxyNew++;

                // attribute to near/far only for comparable cross types
                if (data.comparable && data.dp != null && type != SamplingType.WITHIN) {
                    if (isNearPick) {
                        localNearAttempts++;
                        if (isNew) localNearProxyNew++;
                    } else {
                        localFarAttempts++;
                        if (isNew) localFarProxyNew++;
                    }
                }
            }

            // dynamic update (only comparable cross methods)
            if (data.comparable && data.dp != null && type != SamplingType.WITHIN) {
                data.dp.updatePNear(localNearAttempts, localNearProxyNew, localFarAttempts, localFarProxyNew);
            }

            attempts += localAttempts;
            proxyNew += localProxyNew;

            data.attempts += localAttempts;
            data.proxyNew += localProxyNew;

            double gainRate = (double) localProxyNew / Math.max(1L, localAttempts);
            emaGain = 0.8 * emaGain + 0.2 * gainRate;

            int sizeAfter = evidenceSet.size();
            this.efficiency = (double) (sizeAfter - sizePrior) / Math.max(1, input.getLineCount());
        }
    }

    public FocusedSampling(PredicateBuilder predicates2) {
        super(predicates2);
    }

    public IEvidenceSet buildEvidenceSet(IEvidenceSet evidenceSet, Input input, double efficiencyThreshold) {
        long maxAttempts = Long.getLong("fs.maxAttempts", Long.MAX_VALUE);
        return buildEvidenceSetWithBudget(evidenceSet, input, efficiencyThreshold, maxAttempts);
    }

    public IEvidenceSet buildEvidenceSetWithBudget(IEvidenceSet evidenceSet, Input input, double efficiencyThreshold, long maxAttempts) {
        pairs = predicates.getColumnPairs();
        createSets(pairs);

        // ====== base params ======
        final int NEAR_MAX = Integer.getInteger("fs.nearMax", 3);
        final double P_NEAR = Double.parseDouble(System.getProperty("fs.pNear", "0.65"));
        final double ALPHA_FAR = Double.parseDouble(System.getProperty("fs.alphaFar", "1.2"));
        final int CAND_TRIES = Integer.getInteger("fs.candTries", 3);

        // ====== dynamic pNear params ======
        final boolean DYN_PNEAR = Boolean.parseBoolean(System.getProperty("fs.dynamicPNear", "false"));
        final double PNEAR_MIN = Double.parseDouble(System.getProperty("fs.pNearMin", "0.05"));
        final double PNEAR_MAX = Double.parseDouble(System.getProperty("fs.pNearMax", "0.95"));
        final double PNEAR_ETA = Double.parseDouble(System.getProperty("fs.pNearEta", "0.2"));
        final long PNEAR_WARMUP = Long.getLong("fs.pNearWarmupAttempts", 200_000L);
        final int PNEAR_MIN_SAMPLES = Integer.getInteger("fs.pNearMinSamples", 200);

        // chunk scheduling
        final long CHUNK = Long.getLong("fs.chunk", 50_000L);
        final long WARMUP_CHUNK = Long.getLong("fs.warmupChunk", Math.min(20_000L, CHUNK));

        final boolean CSV = Boolean.parseBoolean(System.getProperty("fs.csv", "true"));
        final boolean VERBOSE = Boolean.parseBoolean(System.getProperty("fs.verbose", "true"));
        final String STRATEGY = System.getProperty("fs.strategy", "NF_compOnly");
        final String DATASET = System.getProperty("dataset", "NA");
        // ==========================

        AttemptBudget budget = new AttemptBudget(maxAttempts);

        SamplingType[] comparableTypes = { SamplingType.WITHIN, SamplingType.BEFORE, SamplingType.LATER };
        SamplingType[] otherTypes      = { SamplingType.WITHIN, SamplingType.OTHER };

        PriorityQueue<SamplingMethod> pq = new PriorityQueue<>((a, b) -> Double.compare(b.emaGain, a.emaGain));
        List<SamplingMethod> allMethods = Collections.synchronizedList(new ArrayList<>());
        List<ColumnData> allCols = Collections.synchronizedList(new ArrayList<>());

        int maxThreads = Math.min(Math.max(1, Runtime.getRuntime().availableProcessors() - 1), input.getColumns().length);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                maxThreads, maxThreads, 0L, TimeUnit.MILLISECONDS,
                new java.util.concurrent.ArrayBlockingQueue<>(100)
        );

        for (ParsedColumn<?> c : input.getColumns()) {
            executor.submit(() -> {
                if (budget.exhausted()) return;

                Map<Object, OrderedCluster> valueMap = new HashMap<>();
                for (int i = 0; i < input.getLineCount(); ++i) {
                    OrderedCluster cluster = valueMap.computeIfAbsent(c.getValue(i), (k) -> new OrderedCluster());
                    cluster.add(i);
                }

                List<OrderedCluster> clusters;
                if (c.isComparableType()) {
                    clusters = valueMap.keySet().stream().sorted().map(valueMap::get).collect(Collectors.toList());
                } else {
                    clusters = new ArrayList<>(valueMap.values());
                }

                for (OrderedCluster cl : clusters) cl.randomize();

                int n = clusters.size();

                int[] clusterW = new int[Math.max(1, n)];
                for (int idx = 0; idx < n; idx++) {
                    OrderedCluster cl = clusters.get(idx);
                    double w = 1000.0 / Math.sqrt(Math.max(1, cl.size()));
                    clusterW[idx] = Math.max(1, (int) Math.round(w));
                }

                DistancePicker dp = null;
                WeightedRandomPicker<Integer> imp = null;

                if (c.isComparableType()) {
                    dp = new DistancePicker(
                            n, NEAR_MAX, P_NEAR, ALPHA_FAR, clusterW, CAND_TRIES,
                            DYN_PNEAR, PNEAR_MIN, PNEAR_MAX, PNEAR_ETA, PNEAR_WARMUP, PNEAR_MIN_SAMPLES
                    );
                } else {
                    imp = new WeightedRandomPicker<>();
                    for (int idx = 0; idx < n; idx++) imp.add(idx, clusterW[idx]);
                }

                ColumnData col = new ColumnData(c.getName(), c.isComparableType(), clusters, dp, imp);
                allCols.add(col);

                SamplingType[] types = col.comparable ? comparableTypes : otherTypes;

                ArrayList<SamplingMethod> methods = new ArrayList<>();
                for (SamplingType type : types) {
                    SamplingMethod m = new SamplingMethod(col, type);
                    methods.add(m);
                    allMethods.add(m);
                }

                // warmup：每个 method 都跑一点，并按 efficiencyThreshold 决定是否入队
                for (SamplingMethod m : methods) {
                    if (budget.exhausted()) break;
                    m.executeChunk(input, evidenceSet, budget, WARMUP_CHUNK);

                    if (!budget.exhausted() && m.emaGain > efficiencyThreshold) {
                        synchronized (pq) { pq.add(m); }
                    }
                }
            });
        }

        // 等待 warmup 完成，再进入 PQ loop（避免 pq 早空导致提前退出）
        executor.shutdown();
        try {
            if (!executor.awaitTermination(6000, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        while (!budget.exhausted()) {
            SamplingMethod m;
            synchronized (pq) { m = pq.poll(); }
            if (m == null) break;

            m.executeChunk(input, evidenceSet, budget, CHUNK);

            if (!budget.exhausted() && m.emaGain > efficiencyThreshold) {
                synchronized (pq) { pq.add(m); }
            }
        }

        // ========================= Summary logs =========================
        long totalAttempts = 0, totalProxyNew = 0;
        long withinA = 0, withinN = 0;
        long beforeA = 0, beforeN = 0;
        long laterA  = 0, laterN  = 0;
        long otherA  = 0, otherN  = 0;

        for (SamplingMethod m : allMethods) {
            totalAttempts += m.attempts;
            totalProxyNew += m.proxyNew;
            switch (m.type) {
                case WITHIN -> { withinA += m.attempts; withinN += m.proxyNew; }
                case BEFORE -> { beforeA += m.attempts; beforeN += m.proxyNew; }
                case LATER  -> { laterA  += m.attempts; laterN  += m.proxyNew; }
                case OTHER  -> { otherA  += m.attempts; otherN += m.proxyNew; }
            }
        }

        long nearPick = 0, farPick = 0;
        long unorderedOtherPick = 0;

        double sumPNear = 0.0;
        int cntPNear = 0;

        for (ColumnData c : allCols) {
            if (c.comparable && c.dp != null) {
                nearPick += c.dp.nearPick;
                farPick  += c.dp.farPick;
                sumPNear += c.dp.getPNear();
                cntPNear++;
            } else {
                unorderedOtherPick += c.unorderedOtherPick;
            }
        }

        long compCrossPick = nearPick + farPick;
        double nearRatio = compCrossPick == 0 ? 0.0 : (1.0 * nearPick / compCrossPick);
        double proxyRate = totalAttempts == 0 ? 0.0 : (1.0 * totalProxyNew / totalAttempts);
        double avgPNearFinal = cntPNear == 0 ? 0.0 : (sumPNear / cntPNear);

        if (VERBOSE) {
            log.info("[NearFar-FS] dataset={} strategy={} maxAttempts={} attempts={} proxyNew={} proxyRate={}",
                    DATASET, STRATEGY, budget.max(), totalAttempts, totalProxyNew, String.format(Locale.ROOT, "%.4f", proxyRate));
            log.info("[NearFar-FS] comparableCrossPick={} near={} far={} nearRatio={} unorderedOtherPick={}",
                    compCrossPick, nearPick, farPick, String.format(Locale.ROOT, "%.3f", nearRatio), unorderedOtherPick);
            log.info("[NearFar-FS] byType: WITHIN({},{}) BEFORE({},{}) LATER({},{}) OTHER({},{})  (format=attempts,proxyNew)",
                    withinA, withinN, beforeA, beforeN, laterA, laterN, otherA, otherN);
            log.info("[NearFar-FS] dynPNear={} pNear0={} pNearAvgFinal={} (min={},max={},eta={},warmupA={},minSamples={})",
                    DYN_PNEAR,
                    String.format(Locale.ROOT, "%.3f", P_NEAR),
                    String.format(Locale.ROOT, "%.3f", avgPNearFinal),
                    String.format(Locale.ROOT, "%.3f", PNEAR_MIN),
                    String.format(Locale.ROOT, "%.3f", PNEAR_MAX),
                    String.format(Locale.ROOT, "%.3f", PNEAR_ETA),
                    PNEAR_WARMUP,
                    PNEAR_MIN_SAMPLES
            );
            log.info("[NearFar-FS] chunk={} warmupChunk={} effTh={}", CHUNK, WARMUP_CHUNK, efficiencyThreshold);
        }

        if (CSV) {
            log.info("[FS-CSV] {},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
                    DATASET, STRATEGY, budget.max(), efficiencyThreshold,
                    NEAR_MAX, P_NEAR, ALPHA_FAR, CAND_TRIES,
                    totalAttempts, totalProxyNew, String.format(Locale.ROOT, "%.6f", proxyRate),
                    compCrossPick, nearPick, farPick, String.format(Locale.ROOT, "%.6f", nearRatio),
                    unorderedOtherPick,
                    withinA, withinN, beforeA, beforeN, laterA, laterN, otherA, otherN
            );
        }

        return evidenceSet;
    }

    private static final Logger log = LoggerFactory.getLogger(FocusedSampling.class);
    private Collection<ColumnPair> pairs;
}
