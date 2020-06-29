package dto;
/* Created by Nimish.Bajaj on 03/06/20 */

import java.util.List;

public class RuleDefinition {
    double weight;
    Long dataset1Id;
    Long dataset2Id;
    List<String> ds1Columns;
    List<String> ds2Columns;
    String algorithm;
    double error_tolerance;
    boolean mandatory;

    public RuleDefinition(double weight, Long dataset1Id, Long dataset2Id, List<String> ds1Columns,
                          List<String> ds2Columns, String algorithm, double error_tolerance, boolean mandatory) {
        this.weight = weight;
        this.dataset1Id = dataset1Id;
        this.dataset2Id = dataset2Id;
        this.ds1Columns = ds1Columns;
        this.ds2Columns = ds2Columns;
        this.algorithm = algorithm;
        this.error_tolerance = error_tolerance;
        this.mandatory = mandatory;
    }


}
