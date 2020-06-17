package dto;
/* Created by Nimish.Bajaj on 03/06/20 */

import java.util.List;

public class Rule {
    List<RuleDefinition> ruleDefinitions;
    double threshold;
    int priority;

    public Rule(List<RuleDefinition> ruleDefinitions, double threshold, int priority) {
        this.ruleDefinitions = ruleDefinitions;
        this.threshold = threshold;
        this.priority = priority;
    }
}
