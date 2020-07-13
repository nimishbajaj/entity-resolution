import dto._

import scala.collection.mutable.ListBuffer

object ObjectGeneration {
  val idCol: String = "identity_id"

  def getRuleDefinitionCondition(identityRuleDefinitionId: Long): (MIdentityRuleDefinitionConditions,
    MIdentityRuleDefinitionConditions) = {
    // define rule definition condition - same as the dataset preparation bit
    identityRuleDefinitionId match {
      case 1 =>
        val rdcD1_1_1 = new MIdentityRuleDefinitionConditions(1, 1, 1, 0, "ecid", "ecid")
        val rdcD1_1_2 = new MIdentityRuleDefinitionConditions(2, 1, 1, 0, "ecid", "ecid")
        (rdcD1_1_1, rdcD1_1_2)

      case 2 =>
        val rdcD2_1_1 = new MIdentityRuleDefinitionConditions(3, 2, 1, 0, "ecid", "zip")
        val rdcD2_1_2 = new MIdentityRuleDefinitionConditions(4, 2, 1, 0, "ecid", "zip")
        (rdcD2_1_1, rdcD2_1_2)

      case 3 =>
        val rdcD2_2_1 = new MIdentityRuleDefinitionConditions(5, 3, 1, 0, "ecid", "city")
        val rdcD2_2_2 = new MIdentityRuleDefinitionConditions(6, 3, 1, 0, "ecid", "city")
        (rdcD2_2_1, rdcD2_2_2)

      case 4 =>
        val rdcD2_3_1 = new MIdentityRuleDefinitionConditions(7, 4, 1, 0, "ecid", "ip,device")
        val rdcD2_3_2 = new MIdentityRuleDefinitionConditions(8, 4, 1, 0, "ecid", "ip,device")
        (rdcD2_3_1, rdcD2_3_2)
    }
  }

  def getRuleDefinition(identityRuleId: Long): List[MIdentityRuleDefinition] = {
    identityRuleId match {
      case 1 =>
        val rdD_1 = new MIdentityRuleDefinition(1, 1, 1, 0)
        List[MIdentityRuleDefinition](rdD_1)

      case 2 =>
        val rdD_2 = new MIdentityRuleDefinition(2, 2, 0.4, 0)
        val rdD_3 = new MIdentityRuleDefinition(3, 2, 0.2, 0)
        val rdD_4 = new MIdentityRuleDefinition(4, 2, 0.4, 0)
        List[MIdentityRuleDefinition](rdD_2, rdD_3, rdD_4)
    }
  }

  def getRule(identityRulesetId: Long): List[MIdentityRule] = {
    identityRulesetId match {
      case 1 =>
        val rD_1 = new MIdentityRule(1, 1, "adobe-adobe-ecid", 1, 1, 1, 1, "ecid", "ecid")

        val rD_2 = new MIdentityRule(2, 1, "adobe-adobe-demo", 2, 0.8, 1, 1, "ecid", "ecid")
        List[MIdentityRule](rD_1, rD_2)
    }
  }

  def getRuleSet(ruleSetId: Long): List[Rule] = {
    val ruleList = new ListBuffer[Rule]()

    getRule(ruleSetId).foreach(mIdentityRule => {
      var ruleDefinitions = new ListBuffer[RuleDefinition]()

      getRuleDefinition(mIdentityRule.getIdentityRuleId) foreach (mIdentityRuleDefinition => {
        var columnIds1: String = null;
        var columnIds2: String = null;

        val (ruleDef1, ruleDef2) = getRuleDefinitionCondition(mIdentityRuleDefinition.getIdentityRuleDefinitionId)
        val ruleDef1datasetId = ruleDef1.getDatasetId
        val ruleDef2datasetId = ruleDef2.getDatasetId

        columnIds1 = mIdentityRule.getDatasetId1 match {
          case ruleDef1datasetId => ruleDef1.getColumnIds
          case ruleDef2datasetId => ruleDef2.getColumnIds
        }

        columnIds2 = mIdentityRule.getDatasetId2 match {
          case ruleDef1datasetId => ruleDef1.getColumnIds
          case ruleDef2datasetId => ruleDef2.getColumnIds
        }

        ruleDefinitions += RuleDefinition(columnIds1, columnIds2, mIdentityRuleDefinition.getWeightage,
          mIdentityRuleDefinition.getTolerance)
      })

      ruleList += Rule(ruleDefinitions.toList, mIdentityRule.getThreshold, mIdentityRule.getPriority, mIdentityRule
        .getDatasetId1, mIdentityRule.getDatasetId2, mIdentityRule.getPrimaryKey1, mIdentityRule.getPrimaryKey2)
    })

    ruleList.toList
  }


  def main(args: Array[String]): Unit = {
    println(getRuleSet(1))
  }
}