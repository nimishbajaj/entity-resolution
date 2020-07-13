package dto;


public class MIdentityRuleDefinitionConditions {

  private long identityRuleDefinitionConditionId;
  private long identityRuleDefinitionId;
  private long datasetId;
  private long operatorId;
  private String primaryKey;
  private String columnIds;
  private String createUser;
  private java.sql.Timestamp createDt;
  private String updateUser;
  private java.sql.Timestamp updateDt;


  public MIdentityRuleDefinitionConditions(long identityRuleDefinitionConditionId, long identityRuleDefinitionId,
                                           long datasetId, long operatorId, String primaryKey, String columnIds) {
    this.identityRuleDefinitionConditionId = identityRuleDefinitionConditionId;
    this.identityRuleDefinitionId = identityRuleDefinitionId;
    this.datasetId = datasetId;
    this.operatorId = operatorId;
    this.primaryKey = primaryKey;
    this.columnIds = columnIds;
  }

  public String getPrimaryKey() {
    return primaryKey;
  }

  public void setPrimaryKey(String primaryKey) {
    this.primaryKey = primaryKey;
  }

  public long getIdentityRuleDefinitionConditionId() {
    return identityRuleDefinitionConditionId;
  }

  public void setIdentityRuleDefinitionConditionId(long identityRuleDefinitionConditionId) {
    this.identityRuleDefinitionConditionId = identityRuleDefinitionConditionId;
  }


  public long getIdentityRuleDefinitionId() {
    return identityRuleDefinitionId;
  }

  public void setIdentityRuleDefinitionId(long identityRuleDefinitionId) {
    this.identityRuleDefinitionId = identityRuleDefinitionId;
  }


  public long getDatasetId() {
    return datasetId;
  }

  public void setDatasetId(long datasetId) {
    this.datasetId = datasetId;
  }


  public long getOperatorId() {
    return operatorId;
  }

  public void setOperatorId(long operatorId) {
    this.operatorId = operatorId;
  }


  public String getColumnIds() {
    return columnIds;
  }

  public void setColumnIds(String columnIds) {
    this.columnIds = columnIds;
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
