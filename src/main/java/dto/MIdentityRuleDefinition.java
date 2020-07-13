package dto;


public class MIdentityRuleDefinition {

  private long identityRuleDefinitionId;
  private long identityRuleId;
  private double weightage;
  private long algorithmId;
  private long tolerance;
  private String createUser;
  private java.sql.Timestamp createDt;
  private String updateUser;
  private java.sql.Timestamp updateDt;

  public MIdentityRuleDefinition(long identityRuleDefinitionId, long identityRuleId, double weightage, long tolerance) {
    this.identityRuleDefinitionId = identityRuleDefinitionId;
    this.identityRuleId = identityRuleId;
    this.weightage = weightage;
    this.tolerance = tolerance;
  }

  public long getIdentityRuleDefinitionId() {
    return identityRuleDefinitionId;
  }

  public void setIdentityRuleDefinitionId(long identityRuleDefinitionId) {
    this.identityRuleDefinitionId = identityRuleDefinitionId;
  }


  public long getIdentityRuleId() {
    return identityRuleId;
  }

  public void setIdentityRuleId(long identityRuleId) {
    this.identityRuleId = identityRuleId;
  }


  public double getWeightage() {
    return weightage;
  }

  public void setWeightage(double weightage) {
    this.weightage = weightage;
  }


  public long getAlgorithmId() {
    return algorithmId;
  }

  public void setAlgorithmId(long algorithmId) {
    this.algorithmId = algorithmId;
  }


  public long getTolerance() {
    return tolerance;
  }

  public void setTolerance(long tolerance) {
    this.tolerance = tolerance;
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
