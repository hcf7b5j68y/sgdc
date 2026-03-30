package sgdc.dc.algorithms.sgdc;

import ch.javasoft.bitset.IBitSet;
import ch.javasoft.bitset.LongBitSet;
import ch.javasoft.bitset.search.NTreeSearch;
import sgdc.dc.coverage.CoverageProvider;
import sgdc.dc.denialcontraints.DenialConstraint;
import sgdc.dc.denialcontraints.DenialConstraintSet;
import sgdc.dc.evidenceset.HashEvidenceSet;
import sgdc.dc.evidenceset.IEvidenceSet;
import sgdc.dc.predicates.Predicate;
import sgdc.dc.predicates.PredicateBuilder;
import sgdc.dc.predicates.sets.PredicateBitSet;
import sgdc.dc.predicates.sets.PredicateSetFactory;
import sgdc.dc.util.DCUtil;
import sgdc.dc.util.Parameters;
import sgdc.dc.util.PruningFactors;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class SGDC {

    private static final class Node {
        final IBitSet S;
        final double ub;
        Node(IBitSet s, double ub) { this.S = s; this.ub = ub; }
    }

    private final PredicateBuilder predicates;
    private final CoverageProvider coverageProvider;
    private final Verification verifier;

    // group bitsets (Hydra-style predicate groups)
    private final List<IBitSet> groupBits;
    private final HashSet<IBitSet> set = new HashSet<>();
    private final HashSet<DenialConstraint> marked = new HashSet<>();
    private final HashSet<DenialConstraint> marked2 = new HashSet<>();
    public SGDC(PredicateBuilder predicates,
                CoverageProvider coverageProvider,
                Verification verifier) {
        this.predicates = predicates;
        this.coverageProvider = coverageProvider;
        this.verifier = verifier;
        this.groupBits = buildPredicateGroupBitsets(predicates);
    }

    // --------------------------- scoring helpers ---------------------------

    private double score(DenialConstraint dc) {
        double cov = coverageProvider.calculate(dc);
        return DCUtil.getScoreWithCoverage(dc, cov);
    }


    public DenialConstraintSet discoverDCs(IEvidenceSet sampleEvidence,
                                           IEvidenceSet initialEvidence,
                                           double tau,
                                           int batchK) {
        long start = System.nanoTime();
        // --- normalize evidence container to HashEvidenceSet (unique evidences) ---
        HashEvidenceSet ev = new HashEvidenceSet();
        for (PredicateBitSet ps : initialEvidence) ev.add(ps);

        // --- build index over evidences (supports dynamic growth) ---
        int P = PredicateBitSet.indexProvider.size();
        EvidenceIndex index = new EvidenceIndex(P);
        index.buildFrom(ev);
        System.out.println("Evidence set size: "+index.size());
        int m = predicates.getPredicateGroups().size();
        // --- best-first queue ordered by admissible upper bound ---
        PriorityQueue<Node> pq = new PriorityQueue<>((a, b) -> Double.compare(b.ub, a.ub));
        IBitSet empty = LongBitSet.FACTORY.create();
        pq.add(new Node(empty, DCUtil.scoreUpper(empty, m)));

        // pending batch for pipeline verify
        DenialConstraintSet pending = new DenialConstraintSet();

        // record “accepted complete predicate sets” (might become infeasible after feedback)
        List<IBitSet> acceptedS = new ArrayList<>();

        // subset check during search (to keep it tight)
        NTreeSearch minimal = new NTreeSearch();

        while (!pq.isEmpty()) {
            Node cur = pq.poll();
            if (cur.ub < tau) break; // certified stop

            IBitSet S = cur.S;



            IBitSet U = index.computeUncovered(S);
            // complete node => candidate DC
            if (U.isEmpty()) {
                // subset check (under current evidence) + score threshold
                if (!isDominatedByValidSubset(minimal, S, index)) {
                    DenialConstraint dc = new DenialConstraint(PredicateSetFactory.create(S));
                    if(!minimal.add(S.clone()))
                        continue;
                    long scoreStart = System.nanoTime();
                    double sc = score(dc);
                    accumulateTime(timings, counts, "score", System.nanoTime() - scoreStart);
                    if (sc >= tau) {
                        // record S; DC itself will be verified in batch (backend adds new evidences)
                        acceptedS.add(S.clone());
                        minimal.add(S.clone());

                        pending.add(dc);
                        marked2.add(dc);
                    }
                }

                if (pending.size() >= batchK) {
                    long minimizeStart = System.nanoTime();
                    pending.minimize();
                    accumulateTime(timings, counts, "minimize", System.nanoTime() - minimizeStart);
                    if (pending.size() < batchK)
                        continue;
                    marked2.addAll(pending.getConstraints());
                    long verifyStart = System.nanoTime();
                    HashEvidenceSet delta = validateAndDelta(sampleEvidence, ev, pending);
                    if (!delta.isEmpty()) {
                        index.addAll(delta);
                    }
                    pending = new DenialConstraintSet();
                    System.err.println("Here");
                    accumulateTime(timings, counts, "validate", System.nanoTime() - verifyStart);
                }
                continue;
            }
            long branchStart = System.nanoTime();

            // pick one uncovered evidence e
            int eId = U.nextSetBit(0);
            IBitSet eBits = index.getEvidenceBits(eId);

            // branch on predicates in (group \ e)\
            for (IBitSet g : groupBits) {
                if (!intersectionEmpty(S, g)) continue; // already used this group

                IBitSet cand = g.clone();
                cand.andNot(eBits); // predicates from this group that are NOT in evidence e
                for (int p = cand.nextSetBit(0); p >= 0; p = cand.nextSetBit(p + 1)) {
                    IBitSet S2 = S.clone();
                    S2.set(p);
                    if(!set.contains(S2))
                        set.add(S2);
                    else
                        continue;
                    double ub2 = DCUtil.scoreUpper(S2, m);
                    if (ub2 >= tau) pq.add(new Node(S2, ub2));
                }
            }


            accumulateTime(timings, counts, "score", System.nanoTime() - branchStart);
        }

        long verifyStart = System.nanoTime();
        // flush remaining candidates
        if (pending.size()!=0) {
            marked2.addAll(pending.getConstraints());
            HashEvidenceSet delta = validateAndDelta(sampleEvidence, ev, pending);
            System.out.println("validated DCs number: "+pending.size());
            if (!delta.isEmpty()) {
                index.addAll(delta);
            }
        }
        accumulateTime(timings, counts, "validate", System.nanoTime() - verifyStart);

        DenialConstraintSet out = new DenialConstraintSet();

        // sort by cardinality ascending so subset check is clean
        acceptedS.sort(Comparator.comparingInt(IBitSet::cardinality));

//        for (IBitSet S : acceptedS) {
//            IBitSet U = index.computeUncovered(S);
//            if(U ==  null || U.isEmpty()){
//                out.add(new DenialConstraint(PredicateSetFactory.create(S)));
//                continue;
//            }
//            int eId = U.nextSetBit(0);
//            IBitSet eBits = index.getEvidenceBits(eId);
//
//            // branch on predicates in (group \ e)\
//            for (IBitSet g : groupBits) {
//                if (!intersectionEmpty(S, g)) continue; // already used this group
//
//                IBitSet cand = g.clone();
//                cand.andNot(eBits); // predicates from this group that are NOT in evidence e
//                for (int p = cand.nextSetBit(0); p >= 0; p = cand.nextSetBit(p + 1)) {
//                    IBitSet S2 = S.clone();
//                    S2.set(p);
//                    //out.add(new DenialConstraint(PredicateSetFactory.create(S2)));
//                }
//            }
//
//        }

        for (IBitSet S : acceptedS) {
            DenialConstraint denialConstraint = new DenialConstraint(PredicateSetFactory.create(S));
            if(marked.contains(denialConstraint) || !marked2.contains(denialConstraint))
                continue;
            out.add(denialConstraint);
        }
        out.minimize();
        PruningFactors factors = new PruningFactors(Parameters.a, Parameters.b, Parameters.tau);
        out = out.pruneWithProvider(factors, coverageProvider);
        out.minimize();
        accumulateTime(timings, counts, "total", System.nanoTime() - start);
        printTimings();
        System.out.println("Evidence set size: "+index.size());
        return out;
    }


    // --------------------------- pipeline verify wrapper ---------------------------

    private HashEvidenceSet validateAndDelta(IEvidenceSet sampleEvidence,
                                             HashEvidenceSet currentEvidence,
                                             DenialConstraintSet set) {

        set.minimize();
        System.out.println(currentEvidence.size());
        HashEvidenceSet returned = verifier.verify(set, sampleEvidence, currentEvidence, marked);
        System.out.println(returned.size());
        // 兼容两种实现：
        // (a) returned 已经是 delta
        // (b) returned 是全量 evidence（含旧 + 新）
        HashEvidenceSet delta = new HashEvidenceSet();
        for (PredicateBitSet ps : returned) {
            if (!currentEvidence.contains(ps)) {
                currentEvidence.add(ps);
                delta.add(ps);
            }
        }
        return delta;
    }

    // --------------------------- minimality helpers ---------------------------

    private boolean isDominatedByValidSubset(NTreeSearch minimal, IBitSet S, EvidenceIndex index) {
        while (true) {
            IBitSet sub = minimal.getSubset(S);
            if (sub == null) return false;

            // subset still feasible under CURRENT evidence set?
            if (index.computeUncovered(sub).isEmpty()) {
                return true; // S has a valid feasible subset => not minimal
            } else {
                // stale subset (became infeasible after evidence updates), remove it lazily
                minimal.remove(sub);
            }
        }
    }

    private static boolean intersectionEmpty(IBitSet a, IBitSet b) {
        IBitSet tmp = a.clone();
        tmp.and(b);
        return tmp.isEmpty();
    }

    private static List<IBitSet> buildPredicateGroupBitsets(PredicateBuilder pb) {
        List<IBitSet> res = new ArrayList<>();
        for (Collection<Predicate> group : pb.getPredicateGroups()) {
            IBitSet bs = LongBitSet.FACTORY.create();
            for (Predicate p : group) {
                bs.or(PredicateSetFactory.create(p).getBitset());
            }
            res.add(bs);
        }
        return res;
    }

    Map<String, Long> timings = new LinkedHashMap<>();
    Map<String, Integer> counts = new HashMap<>();
    private void accumulateTime(Map<String, Long> timings, Map<String, Integer> counts, String key, long nanos) {
        timings.merge(key, nanos, Long::sum);
        counts.merge(key, 1, Integer::sum);
    }
    public void printTimings() {
        System.out.println("=== Performance Report ===");
        AtomicLong total = new AtomicLong(timings.get("total"));
        AtomicLong validate = new AtomicLong(timings.get("validate"));
        AtomicLong score = new AtomicLong(timings.get("score"));
        System.out.printf("%-10s: %8.2f ms%n",
                "total", total.get()/1_000_000.0);
        System.out.printf("%-10s: %8.2f ms%n",
                "score", score.get()/1_000_000.0);
        System.out.printf("%-10s: %8.2f ms%n",
                "validate", validate.get()/1_000_000.0);
        System.out.printf("%-10s: %8.2f ms%n",
                "inversion", (total.get()-validate.get()-score.get())/1_000_000.0);
    }
}
