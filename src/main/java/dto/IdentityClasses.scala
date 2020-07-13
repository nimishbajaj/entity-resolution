package dto

/* Created by Nimish.Bajaj on 03/06/20 */

case class RuleDefinition(columnids1: String, columnids2: String, weight: Double, tolerance: Long)

case class Rule(ruleDefinitions: List[RuleDefinition], threshold: Double, priority: Long, datasetId1: Long,
                datasetId2: Long, pk1: String, pk2: String)

case class RuleSet(rules: List[Rule])

