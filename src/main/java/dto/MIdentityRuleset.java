package dto;


public class MIdentityRuleset {

  private long identityRulesetId;
  private String identityRulesetName;
  private String triggerSchedule;
  private long matchingWorkflowDispatchWindowId;
  private long matchingWorkflowId;
  private long matchRate;
  private long deDuped;
  private String activeFlg;
  private String createUser;
  private java.sql.Timestamp createDt;
  private String updateUser;
  private java.sql.Timestamp updateDt;

  public MIdentityRuleset(long identityRulesetId, String identityRulesetName) {
    this.identityRulesetId = identityRulesetId;
    this.identityRulesetName = identityRulesetName;
  }

  public long getIdentityRulesetId() {
    return identityRulesetId;
  }

  public void setIdentityRulesetId(long identityRulesetId) {
    this.identityRulesetId = identityRulesetId;
  }


  public String getIdentityRulesetName() {
    return identityRulesetName;
  }

  public void setIdentityRulesetName(String identityRulesetName) {
    this.identityRulesetName = identityRulesetName;
  }


  public String getTriggerSchedule() {
    return triggerSchedule;
  }

  public void setTriggerSchedule(String triggerSchedule) {
    this.triggerSchedule = triggerSchedule;
  }


  public long getMatchingWorkflowDispatchWindowId() {
    return matchingWorkflowDispatchWindowId;
  }

  public void setMatchingWorkflowDispatchWindowId(long matchingWorkflowDispatchWindowId) {
    this.matchingWorkflowDispatchWindowId = matchingWorkflowDispatchWindowId;
  }


  public long getMatchingWorkflowId() {
    return matchingWorkflowId;
  }

  public void setMatchingWorkflowId(long matchingWorkflowId) {
    this.matchingWorkflowId = matchingWorkflowId;
  }


  public long getMatchRate() {
    return matchRate;
  }

  public void setMatchRate(long matchRate) {
    this.matchRate = matchRate;
  }


  public long getDeDuped() {
    return deDuped;
  }

  public void setDeDuped(long deDuped) {
    this.deDuped = deDuped;
  }


  public String getActiveFlg() {
    return activeFlg;
  }

  public void setActiveFlg(String activeFlg) {
    this.activeFlg = activeFlg;
  }


  public String getCreateUser() {
    return createUser;
  }

  public void setCreateUser(String createUser) {
    this.createUser = createUser;
  }


  public java.sql.Timestamp getCreateDt() {
    return createDt;
  }

  public void setCreateDt(java.sql.Timestamp createDt) {
    this.createDt = createDt;
  }


  public String getUpdateUser() {
    return updateUser;
  }

  public void setUpdateUser(String updateUser) {
    this.updateUser = updateUser;
  }


  public java.sql.Timestamp getUpdateDt() {
    return updateDt;
  }

  public void setUpdateDt(java.sql.Timestamp updateDt) {
    this.updateDt = updateDt;
  }

}
