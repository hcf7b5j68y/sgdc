package sgdc.dc.util;

import ch.javasoft.bitset.IBitSet;
import com.google.common.util.concurrent.AtomicLongMap;
import sgdc.dc.coverage.CoverageProvider;
import sgdc.dc.denialcontraints.DenialConstraint;
import sgdc.dc.denialcontraints.DenialConstraintSet;
import sgdc.dc.predicates.PartitionRefiner;
import sgdc.dc.predicates.Predicate;
import sgdc.dc.predicates.sets.PredicateBitSet;
import sgdc.dc.predicates.sets.PredicateSetFactory;

import java.util.*;

public class DCUtil {
    public static double a = Parameters.a;
    public static double b = Parameters.b;
    public static Map<Double, Integer> succinctnessMap = new TreeMap<>();

    public static void setAB(double a, double b){
        DCUtil.a = a;
        DCUtil.b = b;
    }

    public void resetMap(){
        succinctnessMap.clear();
    }
    public static double getSuccinctness(DenialConstraint dc){
        Set<String> set = new HashSet<>();
        for(Predicate predicate : dc.getPredicateSet()){
            set.add(predicate.getOperator().getShortString());
            set.add(predicate.getOperand1().getColumn().getName());
            set.add(predicate.getOperand2().getColumn().getName());
        }
        return (double) 4/(2+set.size());
    }

    public static double getCoverage(DenialConstraint dc, AtomicLongMap<PartitionRefiner> selectivityCount, long evidenceCount){

        //return selectivityProduct(dc.getPredicateSet(), selectivityCount, evidenceCount);
        return selectivityEstimate(dc, selectivityCount, evidenceCount);
    }

    public static double getCoverage(DenialConstraint dc, CoverageProvider coverageProvider){
        return coverageProvider.calculate(dc);
        //return selectivityProduct(dc.getPredicateSet(), selectivityCount, evidenceCount);
    }



    private static double selectivityProduct(PredicateBitSet dc, AtomicLongMap<PartitionRefiner> selectivityCount, long evidenceCount){
        double ans = 100D;
        for (Predicate predicate : dc){
            ans *= (double) selectivityCount.get(predicate) / evidenceCount;
        }
        return ans;
    }

    private static double selectivityEstimate(DenialConstraint dc, AtomicLongMap<PartitionRefiner> selectivityCount, long evidenceCount){
        long max = 0;
        int size = 0;
        double ans = 0D;
        for (Predicate predicate : dc.getPredicateSet()){
            max = Math.max(max, selectivityCount.get(predicate));
            size ++;
        }
        double maxP =(double) max / evidenceCount;
        for(int i = 1;i<size;i++){
            ans += Math.pow(maxP, i)*(i+1)/size;
        }
        return Math.min(ans, 1D);
    }

    public static double getScore(DenialConstraint dc, AtomicLongMap<PartitionRefiner> selectivityCount, long evidenceCount, double aa, double bb){
        setAB(aa, bb);
        double succinctness = getSuccinctness(dc);
        succinctnessMap.put(succinctness, succinctnessMap.getOrDefault(succinctness, 0) + 1);
        double coverage = getCoverage(dc, selectivityCount, evidenceCount);
        //System.out.println(succinctness + " " + coverage);
        return a*succinctness+b*coverage;
    }

    public static double getScoreWithCoverage(DenialConstraint dc, double coverage){
        double succinctness = getSuccinctness(dc);
        return a*succinctness+b*(coverage<0?0:coverage);
    }

    public static double getScoreWithProvider(DenialConstraint dc, CoverageProvider coverageProvider){
        double coverage = getCoverage(dc, coverageProvider);
        return getScoreWithCoverage(dc, coverage);
    }

    public static double getScore(DenialConstraint dc, AtomicLongMap<PartitionRefiner> selectivityCount, long evidenceCount){
        double succinctness = getSuccinctness(dc);
        succinctnessMap.put(succinctness, succinctnessMap.getOrDefault(succinctness, 0) + 1);
        double coverage = getCoverage(dc, selectivityCount, evidenceCount);
        //System.out.println(succinctness + " " + coverage);
        return a*succinctness+b*coverage;
    }
    public static void printScore(DenialConstraintSet dcs, AtomicLongMap<PartitionRefiner> selectivityCount, long evidenceCount){
        Map<Double, List<DenialConstraint>> map = new TreeMap<>();
        for(DenialConstraint dc : dcs){
            double score = DCUtil.getScore(dc, selectivityCount, evidenceCount);
            if(map.containsKey(score)){
                map.get(score).add(dc);
            }else{
                List<DenialConstraint> list = new ArrayList<>();
                list.add(dc);
                map.put(score, list);
            }
        }
        for(Map.Entry<Double, List<DenialConstraint>> entry : map.entrySet()){
            System.out.println(entry.getKey() + "; " + entry.getValue().size());
        }
    }

    public static DenialConstraintSet filterBySuccinctness(DenialConstraintSet dcs, double threshold){
        Map<Double, Integer> map = new TreeMap<>();
    	DenialConstraintSet ans = new DenialConstraintSet();
    	for(DenialConstraint dc : dcs){
            double succinctness = getSuccinctness(dc);
            map.put(succinctness, map.getOrDefault(succinctness, 0) + 1);
    		if(succinctness > threshold)
    			ans.add(dc);
    	}
        for(Map.Entry<Double, Integer> entry : map.entrySet()){
            System.out.println(entry.getKey() + " " + entry.getValue());
        }
    	return ans;
    }

    public static List<DenialConstraint> sortDCs(DenialConstraintSet dcs, Map<DenialConstraint, Double> coverageMap, int limit){
        if(limit == -1 || limit == 0 || dcs.size() <= limit)
            return dcs.getConstraints().stream()
                .sorted(Comparator.comparing(dc -> -DCUtil.getScoreWithCoverage(dc, coverageMap.getOrDefault(dc, (double) 1 / dc.size()))))
                .toList();
        else return dcs.getConstraints().stream()
                .sorted(Comparator.comparing(dc -> -DCUtil.getScoreWithCoverage(dc, coverageMap.getOrDefault(dc, (double) 1 / dc.size()))))
                .limit(limit)
                .toList();
    }

    public static List<DenialConstraint> sortDCs(DenialConstraintSet dcs, Map<DenialConstraint, Double> coverageMap){
        return sortDCs(dcs, coverageMap, -1);
    }

    public static double scoreUpper(IBitSet S, int m) {
        DenialConstraint dc = new DenialConstraint(PredicateSetFactory.create(S));
        int n = dc.size();
        double coverage = 0;
        if(dc.size()<=4){
            coverage = 1;
        }else{
            for(int i = n+1;i<=m/2;i++){
                coverage += (double)1/(i*(i+1))-(double)4/((i+3)*(i+5));
            }

        }
        return DCUtil.getScoreWithCoverage(dc, coverage);
    }

    public static double scoreUpper(IBitSet S, CoverageProvider coverageProvider) {
        DenialConstraint dc = new DenialConstraint(PredicateSetFactory.create(S));
        double coverage;
        if(dc.size()<=6){
            coverage = 1;
        }else{
            //System.out.println(coverageProvider.calculate(new DenialConstraint(new PredicateBitSet(S))));
            coverage = 0;
        }
        return DCUtil.getScoreWithCoverage(dc, coverage);
//        double covUpper = dc.size()<=3 ? 1.0 : coverageProvider.calculate(dc); // always safe
//        return DCUtil.getScoreWithCoverage(dc, covUpper);
    }
}
