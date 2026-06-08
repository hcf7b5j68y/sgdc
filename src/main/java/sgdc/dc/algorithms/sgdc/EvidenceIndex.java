package sgdc.dc.algorithms.sgdc;

import ch.javasoft.bitset.IBitSet;
import ch.javasoft.bitset.LongBitSet;
import sgdc.dc.evidenceset.HashEvidenceSet;
import sgdc.dc.predicates.sets.PredicateBitSet;

import java.util.ArrayList;
import java.util.List;

public class EvidenceIndex {
    private final int predCount;

    // evidence id -> evidence bitset (satisfied predicates)
    public final List<IBitSet> evidences = new ArrayList<>();

    // postings[p] : bitset of evidence ids whose evidence contains predicate p
    private final IBitSet[] postings;

    // all evidence ids currently in index
    private final IBitSet allEvidenceIds = LongBitSet.FACTORY.create();

    public EvidenceIndex(int predCount) {
        this.predCount = predCount;
        this.postings = new IBitSet[predCount];
        for (int i = 0; i < predCount; i++) {
            postings[i] = LongBitSet.FACTORY.create();
        }
    }

    public int size() {
        return evidences.size();
    }

    public IBitSet getEvidenceBits(int evidenceId) {
        return evidences.get(evidenceId);
    }

    /** Build from an initial evidence set (unique evidences). */
    public void buildFrom(HashEvidenceSet ev) {
        for (PredicateBitSet ps : ev) {
            addEvidence(ps.getBitset());
        }
    }

    /** Add all new evidences (delta). */
    public void addAll(HashEvidenceSet delta) {
        for (PredicateBitSet ps : delta) {
            addEvidence(ps.getBitset());
        }
    }

    /** Append one evidence bitset and update postings. */
    public void addEvidence(IBitSet eBits) {
        int id = evidences.size();
        evidences.add(eBits);
        allEvidenceIds.set(id);

        for (int p = eBits.nextSetBit(0); p >= 0; p = eBits.nextSetBit(p + 1)) {
            if (p < predCount) postings[p].set(id);
        }
    }

    /**
     * U(S) = { e | S ⊆ e }  (equivalently S ∩ (P\e)=∅)
     * Implemented as intersection of postings of all predicates in S.
     */
    public IBitSet computeUncovered(IBitSet S) {
        IBitSet u = allEvidenceIds.clone();
        for (int p = S.nextSetBit(0); p >= 0; p = S.nextSetBit(p + 1)) {
            u.and(postings[p]);
            if (u.isEmpty()) break;
        }
        return u;
    }
}
