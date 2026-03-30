package sgdc.dc.algorithms.sgdc;

import ch.javasoft.bitset.IBitSet;
import ch.javasoft.bitset.LongBitSet;
import ch.javasoft.bitset.search.TreeSearch;
import sgdc.dc.denialcontraints.DenialConstraint;
import sgdc.dc.denialcontraints.DenialConstraintSet;
import sgdc.dc.evidenceset.IEvidenceSet;
import sgdc.dc.helpers.IndexProvider;
import sgdc.dc.predicates.Predicate;
import sgdc.dc.predicates.PredicateBuilder;
import sgdc.dc.predicates.sets.PredicateBitSet;
import sgdc.dc.predicates.sets.PredicateSetFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * SBLEI: Score-Bounded Lazy Evidence Inversion (best-first),
 * with batch pipeline verification (backend-adapter) and optional evidence feedback.
 *
 * Key points vs PrefixMinimalCoverSearch:
 * - Lazy node expansion: nodes are partial predicate-sets S
 * - U(S) computed on-demand via evidence index: U(S) = { e | S ⊆ e }  (e evidences that still violate DC(S))
 * - Best-first by admissible ScoreUpper(S) with certified pruning (stop when best upper < tau)
 * - Batch verify when pending reaches K (no Budget Control here)
 */
public class SBLEISearch {

    private static final Logger log = LoggerFactory.getLogger(SBLEISearch.class);

    // ---------- Plug-in interfaces (you connect to your existing score + verifier) ----------

    /** Score and admissible upper bound on any extension of S up to mMax. */
    public interface Scorer {
        double score(IBitSet S);                 // Score(phi(S))
        double upperBound(IBitSet S, int mMax);  // admissible ScoreUpper(S)
    }

    /** Backend batch verifier. You can wrap Hydra's pipeline checker here. */
    public interface BatchVerifier {
        VerificationResult verifyBatch(List<DenialConstraint> batch);
    }

    /** Verification outputs (extend as needed: stats, violations, etc.). */
    public static class VerificationResult {
        public final DenialConstraintSet verified;   // verified DCs + stats you want to store
        public final IEvidenceSet newEvidence;       // evidences derived from newly found violations (nullable)

        public VerificationResult(DenialConstraintSet verified, IEvidenceSet newEvidence) {
            this.verified = verified;
            this.newEvidence = newEvidence;
        }
    }

    // ---------- SBLEI configuration ----------
    private final PredicateBuilder predicates;
    private final Scorer scorer;
    private final BatchVerifier verifier;
    private final double tau;
    private final int mMax;
    private final int batchK;

    // group constraint (Hydra-style: at most one predicate per group)
    private final int[] predToGroup;
    private final int groupCount;

    private static final IndexProvider<Predicate> indexProvider = PredicateBitSet.indexProvider;

    public SBLEISearch(PredicateBuilder predicates,
                       Scorer scorer,
                       BatchVerifier verifier,
                       double tau,
                       int mMax,
                       int batchK) {
        this.predicates = predicates;
        this.scorer = scorer;
        this.verifier = verifier;
        this.tau = tau;
        this.mMax = mMax;
        this.batchK = batchK;

        // build pred -> group mapping (same logic as your bitsetList construction)
        int pCount = indexProvider.size();
        this.predToGroup = new int[pCount];
        Arrays.fill(this.predToGroup, -1);

        int g = 0;
        for (Collection<Predicate> group : predicates.getPredicateGroups()) {
            for (Predicate p : group) {
                int id = indexProvider.getIndex(p);
                predToGroup[id] = g;
            }
            g++;
        }
        this.groupCount = g;
    }

    // ---------- Public API ----------
    public DenialConstraintSet discover(IEvidenceSet evidenceSet) {
        // 1) collect evidences and optionally minimize (keep maximal evidences)
        List<IBitSet> evidences = collectEvidenceBitsets(evidenceSet);
        evidences = minimizeMaximalEvidences(evidences);

        // 2) build evidence index for U(S) = intersection_{p in S} E_p
        EvidenceIndex index = new EvidenceIndex(indexProvider.size());
        for (IBitSet e : evidences) index.addEvidence(e);

        // 3) best-first PQ by ScoreUpper
        PriorityQueue<Node> pq = new PriorityQueue<>(Comparator.comparingDouble((Node n) -> n.upper).reversed());
        Node root = new Node(LongBitSet.FACTORY.create(), new BitSet(groupCount), scorer.upperBound(LongBitSet.FACTORY.create(), mMax));
        if (root.upper >= tau) pq.add(root);

        // 4) minimality cache over emitted complete sets (evidence-minimal outputs)
        MinimalSetStore minimals = new MinimalSetStore();

        // 5) pending batch
        List<DenialConstraint> pending = new ArrayList<>();

        DenialConstraintSet out = new DenialConstraintSet();

        while (!pq.isEmpty()) {
            Node cur = pq.poll();
            if (cur.upper < tau) break; // certified: no remaining node can reach tau

            if (cur.size() > mMax) continue;

            // compute U(S) on-demand: evidences that still violate phi(S), i.e., S ⊆ e
            IBitSet U = index.violatingEvidences(cur.S);

            if (U.isEmpty()) {
                // complete candidate
                if (cur.size() == 0) continue; // typically ignore empty conjunction unless you want it

                // evidence-minimality filter: keep only subset-minimal feasible sets
                if (minimals.hasSubset(cur.S)) continue;
                minimals.removeSupersetsOf(cur.S);
                minimals.add(cur.S);

                double s = scorer.score(cur.S);
                if (s >= tau) {
                    pending.add(toDC(cur.S));
                    if (pending.size() >= batchK) {
                        flushBatch(pending, out, index);
                        pending.clear();
                    }
                }
                continue;
            }

            // expand: pick one uncovered/violating evidence e in U(S)
            int eid = U.nextSetBit(0);
            IBitSet e = index.getEvidence(eid);

            // branch on all p \in (P \ e), excluding already-chosen predicates and respecting group constraint
            IBitSet cand = index.allPredicates.clone();
            cand.andNot(e);
            cand.andNot(cur.S);

            for (int p = cand.nextSetBit(0); p >= 0; p = cand.nextSetBit(p + 1)) {
                if (cur.size() + 1 > mMax) break;

                int gid = predToGroup[p];
                if (gid >= 0 && cur.usedGroups.get(gid)) continue;

                IBitSet childS = cur.S.clone();
                childS.set(p);

                BitSet childGroups = (BitSet) cur.usedGroups.clone();
                if (gid >= 0) childGroups.set(gid);

                double upper = scorer.upperBound(childS, mMax);
                if (upper >= tau) {
                    pq.add(new Node(childS, childGroups, upper));
                }
            }
        }

        // flush remaining
        if (!pending.isEmpty()) {
            flushBatch(pending, out, index);
            pending.clear();
        }

        return out;
    }

    // ---------- Batch verify + optional evidence feedback ----------
    private void flushBatch(List<DenialConstraint> batch, DenialConstraintSet out, EvidenceIndex index) {
        VerificationResult res = verifier.verifyBatch(batch);
        if (res == null) return;

        if (res.verified != null) {
            res.verified.forEach(out::add);
        }

        // Optional closed-loop: inject new evidences derived from violations
        // Important: we compute U(S) on-demand, so queued nodes remain semantically correct after index grows.
        if (res.newEvidence != null) {
            for (PredicateBitSet ps : res.newEvidence) {
                index.addEvidence(ps.getBitset());
            }
        }
    }

    // ---------- Helpers ----------
    private DenialConstraint toDC(IBitSet S) {
        return new DenialConstraint(PredicateSetFactory.create(S));
    }

    private List<IBitSet> collectEvidenceBitsets(IEvidenceSet evidenceSet) {
        List<IBitSet> list = new ArrayList<>();
        for (PredicateBitSet ps : evidenceSet) list.add(ps.getBitset());
        return list;
    }

    /**
     * Keep only maximal evidences (by set inclusion). This is the same spirit as your minimize():
     * if e1 ⊆ e2, then complement(e2) ⊆ complement(e1), so hitting complement(e2) also hits complement(e1).
     */
    private List<IBitSet> minimizeMaximalEvidences(final List<IBitSet> evs) {
        evs.sort((o1, o2) -> {
            int c = Integer.compare(o2.cardinality(), o1.cardinality());
            return c != 0 ? c : o2.compareTo(o1);
        });

        TreeSearch tree = new TreeSearch();
        for (IBitSet e : evs) {
            if (tree.findSuperSet(e) != null) continue;
            tree.getAndRemoveGeneralizations(e);
            tree.add(e);
        }

        ArrayList<IBitSet> out = new ArrayList<>();
        tree.forEach(out::add);
        return out;
    }

    // ---------- Internal data structures ----------

    private static final class Node {
        final IBitSet S;
        final BitSet usedGroups;
        final double upper;

        Node(IBitSet S, BitSet usedGroups, double upper) {
            this.S = S;
            this.usedGroups = usedGroups;
            this.upper = upper;
        }

        int size() {
            return S.cardinality();
        }
    }

    /**
     * Evidence index for fast U(S) computation:
     * U(S) = {evidence id | evidence contains all bits in S} = intersection_{p in S} E_p.
     */
    private static final class EvidenceIndex {
        final int predCount;
        final List<IBitSet> evidences = new ArrayList<>();
        final IBitSet[] evidenceMaskByPred; // evidenceMaskByPred[p] has bit i iff evidences[i] contains predicate p
        final IBitSet allEvidenceIds = LongBitSet.FACTORY.create();
        final IBitSet allPredicates = LongBitSet.FACTORY.create();

        EvidenceIndex(int predCount) {
            this.predCount = predCount;
            this.evidenceMaskByPred = new IBitSet[predCount];
            for (int p = 0; p < predCount; p++) {
                evidenceMaskByPred[p] = LongBitSet.FACTORY.create();
                allPredicates.set(p);
            }
        }

        void addEvidence(IBitSet e) {
            int id = evidences.size();
            evidences.add(e);
            allEvidenceIds.set(id);
            for (int p = e.nextSetBit(0); p >= 0; p = e.nextSetBit(p + 1)) {
                if (p < predCount) evidenceMaskByPred[p].set(id);
            }
        }

        IBitSet violatingEvidences(IBitSet S) {
            // empty S violates all evidences
            if (S.isEmpty()) return allEvidenceIds.clone();

            IBitSet res = allEvidenceIds.clone();
            for (int p = S.nextSetBit(0); p >= 0; p = S.nextSetBit(p + 1)) {
                res.and(evidenceMaskByPred[p]);
                if (res.isEmpty()) break;
            }
            return res;
        }

        IBitSet getEvidence(int id) {
            return evidences.get(id);
        }
    }

    /**
     * Evidence-minimal output filter:
     * - discard S if any accepted minimal set is a subset of S
     * - when accepting S, remove any accepted set that is a strict superset of S
     *
     * Implemented using your NTreeSearch which already supports subset test + superset enumeration.
     */
    private static final class MinimalSetStore {
        private final ch.javasoft.bitset.search.NTreeSearch tree = new ch.javasoft.bitset.search.NTreeSearch();

        boolean hasSubset(IBitSet S) {
            return tree.containsSubset(S);
        }

        void add(IBitSet S) {
            tree.add(S);
        }

        void removeSupersetsOf(IBitSet S) {
            List<IBitSet> toRemove = new ArrayList<>();
            tree.forEachSuperSet(S, toRemove::add);
            for (IBitSet sup : toRemove) {
                // remove exact superset bitset from tree
                tree.remove(sup);
            }
        }
    }
}
