package sgdc.dc.util;

public class Parameters {
    public static double a = 0.6d;
    public static double b = 1-a;
    public static double tau = 0.55;
    public static double efficiencyThreshold = 0.04d;
    public static int row = -1;
    public static int col = -1;
    public static void setParameters(double a, double tau, double efficiencyThreshold) {
        Parameters.a = a;
        Parameters.b = 1-a;
        Parameters.tau = tau;
        Parameters.efficiencyThreshold = efficiencyThreshold;
    }
}
