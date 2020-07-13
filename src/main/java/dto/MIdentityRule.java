package dto;


public class MIdentityRule {

  private long identityRuleId;
  private long identityRulesetId;
  private String identityRuleName;
  private long priority;
  private double threshold;
  private long datasetId1;
  private long datasetId2;
  private String primaryKey1;
  private String primaryKey2;
  private String createUser;
  private java.sql.Timestamp createDt;
  private String updateUser;
  private java.sql.Timestamp updateDt;

  public MIdentityRule(long identityRuleId, long identityRulesetId, String identityRuleName, long priority,
                       double threshold, long datasetId1, long datasetId2, String primaryKey1, String primaryKey2) {
    this.identityRuleId = identityRuleId;
    this.identityRulesetId = identityRulesetId;
    this.identityRuleName = identityRuleName;
    this.priority = priority;
    this.threshold = threshold;
    this.datasetId1 = datasetId1;
    this.datasetId2 = datasetId2;
    this.primaryKey1 = primaryKey1;
    this.primaryKey2 = primaryKey2;
  }

  public String getPrimaryKey1() {
    return primaryKey1;
  }

  public void setPrimaryKey1(String primaryKey1) {
    this.primaryKey1 = primaryKey1;
  }

  public String getPrimaryKey2() {
    return primaryKey2;
  }

  public void setPrimaryKey2(String primaryKey2) {
    this.primaryKey2 = primaryKey2;
  }

  public long getDatasetId1() {
    return datasetId1;
  }

  public void setDatasetId1(long datasetId1) {
    this.datasetId1 = datasetId1;
  }

  public long getDatasetId2() {
    return datasetId2;
  }

  public void setDatasetId2(long datasetId2) {
    this.datasetId2 = datasetId2;
  }

  public long getIdentityRuleId() {
    return identityRuleId;
  }

  public void setIdentityRuleId(long identityRuleId) {
    this.identityRuleId = identityRuleId;
  }


  public long getIdentityRulesetId() {
    return identityRulesetId;
  }

  public void setIdentityRulesetId(long identityRulesetId) {
    this.identityRulesetId = identityRulesetId;
  }


  public String getIdentityRuleName() {
    return identityRuleName;
  }

  public void setIdentityRuleName(String identityRuleName) {
    this.identityRuleName = identityRuleName;
  }


  public long getPriority() {
    return priority;
  }

  public void setPriority(long priority) {
    this.priority = priority;
  }


  public double getThreshold() {
    return threshold;
  }

  public void setThreshold(double threshold) {
    this.threshold = threshold;
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
