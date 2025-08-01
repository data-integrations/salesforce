/*
 * Copyright © 2025 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.sforce.soap.partner;

import com.sforce.soap.partner.fault.ApiFault;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import io.cdap.plugin.salesforce.plugin.source.batch.util.SalesforceQueryExecutionException;

import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

/**
 * A utility class that wraps Salesforce PartnerConnection calls with retry logic using Failsafe.
 */
public class PartnerConnectionRetryWrapper extends PartnerConnection {

  private final RetryPolicy<Object> retryPolicy;
  private static final Set<String> RETRYABLE_CONNECTION_ERRORS = new HashSet<>(Arrays.asList(
    "connection refused",
    "connection timed out",
    "failed to send request",
    "socket timeout",
    "read timed out",
    "connection reset"
  ));

  private static final Set<String> RETRYABLE_API_FAULT_ERRORS = new HashSet<>(Collections.singletonList(
    "SERVER_UNAVAILABLE"
  ));
  private final PartnerConnection delegate;

  public PartnerConnectionRetryWrapper(PartnerConnection delegate, RetryPolicy<Object> retryPolicy) throws
    ConnectionException {
    super(delegate.getConfig());
    this.delegate = delegate;
    this.retryPolicy = retryPolicy;
  }

  @Override
  public DescribeTab[] describeAllTabs() throws ConnectionException {
    return executeWithRetry(delegate::describeAllTabs, "The describeAllTabs method returned a null result.");
  }

  @Override
  public DescribeDataCategoryGroupStructureResult[]
  describeDataCategoryGroupStructures(DataCategoryGroupSobjectTypePair[] pairs, boolean topCategoriesOnly) throws
    ConnectionException {
    return executeWithRetry(() -> delegate.describeDataCategoryGroupStructures(pairs, topCategoriesOnly),
                            "The describeDataCategoryGroupStructures method returned a null result.");
  }

  @Override
  public DescribeDataCategoryGroupResult[] describeDataCategoryGroups(String[] sObjectType) throws ConnectionException {
    return executeWithRetry(() -> delegate.describeDataCategoryGroups(sObjectType),
                            "The describeDataCategoryGroups method returned a null result.");
  }

  @Override
  public FindDuplicatesResult[] findDuplicates(SObject[] sObjects) throws ConnectionException {
    return executeWithRetry(() -> delegate.findDuplicates(sObjects),
                            "The findDuplicates method returned a null result.");
  }

  @Override
  public ProcessResult[] process(ProcessRequest[] actions) throws ConnectionException {
    return executeWithRetry(() -> delegate.process(actions),
                            "The process method returned a null result.");
  }

  @Override
  public DescribeGlobalResult describeGlobal() throws ConnectionException {
    return executeWithRetry(delegate::describeGlobal, "The describeGlobal() method returned a null result.");
  }

  @Override
  public GetUserInfoResult getUserInfo() throws ConnectionException {
    return executeWithRetry(delegate::getUserInfo, "The UserInfo or OrganizationId is null.");
  }

  @Override
  public DescribeGlobalTheme describeGlobalTheme() throws ConnectionException {
    return executeWithRetry(delegate::describeGlobalTheme,
                            "The describeGlobalTheme method returned a null result.");
  }

  @Override
  public DescribeApprovalLayoutResult describeApprovalLayout(String sObjectType, String[] approvalProcessNames) throws
    ConnectionException {
    return executeWithRetry(() -> delegate.describeApprovalLayout(sObjectType, approvalProcessNames),
                            "The describeApprovalLayout method returned a null result.");
  }

  @Override
  public DescribeCompactLayout[] describePrimaryCompactLayouts(String[] sObjectTypes) throws ConnectionException {
    return executeWithRetry(() -> delegate.describePrimaryCompactLayouts(sObjectTypes),
                            "The describePrimaryCompactLayouts method returned a null result.");
  }

  @Override
  public QueryResult queryMore(String queryLocator) throws ConnectionException {
    return executeWithRetry(() -> delegate.queryMore(queryLocator),
                            "The QueryMore returned a null result for locator used in query: " + queryLocator
    );
  }

  @Override
  public DescribeSearchableEntityResult[] describeSearchableEntities(boolean includeOnlyEntitiesWithTabs) throws
    ConnectionException {
    return executeWithRetry(() -> delegate.describeSearchableEntities(includeOnlyEntitiesWithTabs),
                            "The describeSearchableEntities method returned a null result.");
  }

  @Override
  public DescribeLayoutResult describeLayout(String sObjectType, String layoutName, String[] recordTypeIds) throws
    ConnectionException {
    return executeWithRetry(() -> delegate.describeLayout(sObjectType, layoutName, recordTypeIds),
                            "The describeLayout method returned a null result.");
  }

  @Override
  public DescribeAppMenuResult describeAppMenu(AppMenuType appMenuType, String networkId) throws ConnectionException {
    return executeWithRetry(() -> delegate.describeAppMenu(appMenuType, networkId),
                            "The describeAppMenu method returned a null result.");
  }

  @Override
  public DescribeLookupLayoutResult[] describeLookupLayouts(String[] sObjectType) throws ConnectionException {
    return executeWithRetry(() -> delegate.describeLookupLayouts(sObjectType),
                            "The describeLookupLayouts method returned a null result.");
  }

  @Override
  public LeadConvertResult[] convertLead(LeadConvert[] leadConverts) throws ConnectionException {
    return executeWithRetry(() -> delegate.convertLead(leadConverts),
                            "The convertLead method returned a null result.");
  }

  @Override
  public DescribeSoqlListViewResult describeSObjectListViews(String sObjectType, boolean recentsOnly,
                                                             ListViewIsSoqlCompatible isSoqlCompatible,
                                                             int limit, int offset) throws ConnectionException {
    return executeWithRetry(
      () -> delegate.describeSObjectListViews(sObjectType, recentsOnly, isSoqlCompatible, limit, offset),
      "The describeSObjectListViews method returned a null result.");
  }

  @Override
  public DeleteResult[] delete(String[] ids) throws ConnectionException {
    return executeWithRetry(() -> delegate.delete(ids),
                            "The delete method returned a null result.");
  }

  @Override
  public LoginResult login(String username, String password) throws ConnectionException {
    return executeWithRetry(() -> delegate.login(username, password),
                            "The login method returned a null result.");
  }

  @Override
  public QueryResult queryAll(String queryString) throws ConnectionException {
    return executeWithRetry(() -> delegate.queryAll(queryString),
                            "The queryAll method returned a null result.");
  }

  @Override
  public SaveResult[] update(SObject[] sObjects) throws ConnectionException {
    return executeWithRetry(() -> delegate.update(sObjects),
                            "The update method returned a null result.");
  }

  @Override
  public EmptyRecycleBinResult[] emptyRecycleBin(String[] ids) throws ConnectionException {
    return executeWithRetry(() -> delegate.emptyRecycleBin(ids),
                            "The emptyRecycleBin method returned a null result.");
  }

  @Override
  public DescribeCompactLayoutsResult describeCompactLayouts(String sObjectType, String[] recordTypeIds) throws
    ConnectionException {
    return executeWithRetry(() -> delegate.describeCompactLayouts(sObjectType, recordTypeIds),
                            "The describeCompactLayouts method returned a null result.");
  }

  @Override
  public ChangeOwnPasswordResult changeOwnPassword(String oldPassword, String newPassword) throws ConnectionException {
    return executeWithRetry(() -> delegate.changeOwnPassword(oldPassword, newPassword),
                            "The changeOwnPassword method returned a null result.");
  }

  @Override
  public DescribeSoqlListViewResult describeSoqlListViews(DescribeSoqlListViewsRequest request) throws
    ConnectionException {
    return executeWithRetry(() -> delegate.describeSoqlListViews(request),
                            "The describeSoqlListViews method returned a null result.");
  }

  @Override
  public DescribePathAssistantsResult describePathAssistants(String sObjectType, String picklistValue,
                                                             String[] recordTypeIds) throws ConnectionException {
    return executeWithRetry(() -> delegate.describePathAssistants(sObjectType, picklistValue, recordTypeIds),
                            "The describePathAssistants method returned a null result.");
  }

  @Override
  public DescribeAvailableQuickActionResult[] describeAvailableQuickActions(String contextType) throws
    ConnectionException {
    return executeWithRetry(() -> delegate.describeAvailableQuickActions(contextType),
                            "The describeAvailableQuickActions method returned a null result.");
  }

  @Override
  public GetDeletedResult getDeleted(String sObjectType, Calendar startDate, Calendar endDate) throws
    ConnectionException {
    return executeWithRetry(() -> delegate.getDeleted(sObjectType, startDate, endDate),
                            "The getDeleted method returned a null result.");
  }

  @Override
  public DescribeTabSetResult[] describeTabs() throws ConnectionException {
    return executeWithRetry(delegate::describeTabs,
                            "The describeTabs method returned a null result.");
  }

  @Override
  public QuickActionTemplateResult[] retrieveMassQuickActionTemplates(String quickActionName, String[] contextIds)
    throws ConnectionException {
    return executeWithRetry(() -> delegate.retrieveMassQuickActionTemplates(quickActionName, contextIds),
                            "The retrieveMassQuickActionTemplates method returned a null result.");
  }

  @Override
  public SearchResult search(String searchString) throws ConnectionException {
    return executeWithRetry(() -> delegate.search(searchString),
                            "The search method returned a null result.");
  }

  @Override
  public SendEmailResult[] sendEmail(Email[] messages) throws ConnectionException {
    return executeWithRetry(() -> delegate.sendEmail(messages),
                            "The sendEmail method returned a null result.");
  }

  @Override
  public GetUpdatedResult getUpdated(String sObjectType, Calendar startDate, Calendar endDate) throws
    ConnectionException {
    return executeWithRetry(() -> delegate.getUpdated(sObjectType, startDate, endDate),
                            "The getUpdated method returned a null result.");
  }

  @Override
  public SendEmailResult[] sendEmailMessage(String[] ids) throws ConnectionException {
    return executeWithRetry(() -> delegate.sendEmailMessage(ids),
                            "The sendEmailMessage method returned a null result.");
  }

  @Override
  public DescribeQuickActionResult[] describeQuickActionsForRecordType(String[] quickActions, String recordTypeId)
    throws ConnectionException {
    return executeWithRetry(() -> delegate.describeQuickActionsForRecordType(quickActions, recordTypeId),
                            "The describeQuickActionsForRecordType method returned a null result.");
  }

  @Override
  public RenderEmailTemplateResult[] renderEmailTemplate(RenderEmailTemplateRequest[] renderRequests) throws
    ConnectionException {
    return executeWithRetry(() -> delegate.renderEmailTemplate(renderRequests),
                            "The renderEmailTemplate method returned a null result.");
  }

  @Override
  public UpsertResult[] upsert(String externalIDFieldName, SObject[] sObjects) throws ConnectionException {
    return executeWithRetry(() -> delegate.upsert(externalIDFieldName, sObjects),
                            "The upsert method returned a null result.");
  }

  @Override
  public QueryResult query(String queryString) throws ConnectionException {
    return executeWithRetry(() -> delegate.query(queryString),
                            "The query method returned a null result: " + queryString);
  }

  @Override
  public DescribeQuickActionResult[] describeQuickActions(String[] quickActions) throws ConnectionException {
    return executeWithRetry(() -> delegate.describeQuickActions(quickActions),
                            "The describeQuickActions method returned a null result.");
  }

  @Override
  public PerformQuickActionResult[] performQuickActions(PerformQuickActionRequest[] quickActions) throws
    ConnectionException {
    return executeWithRetry(() -> delegate.performQuickActions(quickActions),
                            "The performQuickActions method returned a null result.");
  }

  @Override
  public DescribeSObjectResult[] describeSObjects(String[] sObjectType) throws ConnectionException {
    return executeWithRetry(() -> delegate.describeSObjects(sObjectType),
                            "The describeSObjects returned a null result: " + Arrays.stream(sObjectType).collect(
                              Collectors.toSet())
    );
  }

  @Override
  public KnowledgeSettings describeKnowledgeSettings() throws ConnectionException {
    return executeWithRetry(delegate::describeKnowledgeSettings,
                            "The describeKnowledgeSettings method returned a null result.");
  }

  @Override
  public UndeleteResult[] undelete(String[] ids) throws ConnectionException {
    return executeWithRetry(() -> delegate.undelete(ids),
                            "The undelete method returned a null result.");
  }

  @Override
  public SObject[] retrieve(String fieldList, String sObjectType, String[] ids) throws ConnectionException {
    return executeWithRetry(() -> delegate.retrieve(fieldList, sObjectType, ids),
                            "The Retrieve returned a null result for sObject: " + sObjectType
    );
  }

  @Override
  public DescribeThemeResult describeTheme(String[] sobjectType) throws ConnectionException {
    return executeWithRetry(() -> delegate.describeTheme(sobjectType),
                            "The describeTheme method returned a null result.");
  }

  @Override
  public DeleteByExampleResult[] deleteByExample(SObject[] sObjects) throws ConnectionException {
    return executeWithRetry(() -> delegate.deleteByExample(sObjects),
                            "The DeleteByExampleResult method returned a null result.");
  }

  @Override
  public DescribeNounResult[] describeNouns(String[] nouns, boolean onlyRenamed, boolean includeFields) throws
    ConnectionException {
    return executeWithRetry(() -> delegate.describeNouns(nouns, onlyRenamed, includeFields),
                            "The DescribeNounResult method returned a null result.");
  }

  @Override
  public FindDuplicatesResult[] findDuplicatesByIds(String[] ids) throws ConnectionException {
    return executeWithRetry(() -> delegate.findDuplicatesByIds(ids),
                            "The findDuplicatesByIds method returned a null result.");
  }

  @Override
  public ExecuteListViewResult executeListView(ExecuteListViewRequest request) throws ConnectionException {
    return executeWithRetry(() -> delegate.executeListView(request),
                            "The executeListView method returned a null result.");
  }

  @Override
  public RenderStoredEmailTemplateResult renderStoredEmailTemplate(RenderStoredEmailTemplateRequest request) throws
    ConnectionException {
    return executeWithRetry(() -> delegate.renderStoredEmailTemplate(request),
                            "The renderStoredEmailTemplate method returned a null result.");
  }

  @Override
  public DescribeVisualForceResult describeVisualForce(boolean includeAllDetails, String namespacePrefix) throws
    ConnectionException {
    return executeWithRetry(() -> delegate.describeVisualForce(includeAllDetails, namespacePrefix),
                            "The describeVisualForce method returned a null result.");
  }

  @Override
  public DescribeSObjectResult describeSObject(String sObjectType) throws ConnectionException {
    return executeWithRetry(() -> delegate.describeSObject(sObjectType),
                            "The describeSObject returned a null result: " + sObjectType);
  }

  @Override
  public GetServerTimestampResult getServerTimestamp() throws ConnectionException {
    return executeWithRetry(delegate::getServerTimestamp,
                            "The getServerTimestamp method returned a null result.");
  }

  @Override
  public QuickActionTemplateResult[] retrieveQuickActionTemplates(String[] quickActionNames, String contextId) throws
    ConnectionException {
    return executeWithRetry(() -> delegate.retrieveQuickActionTemplates(quickActionNames, contextId),
                            "The retrieveQuickActionTemplates method returned a null result.");
  }

  @Override
  public SetPasswordResult setPassword(String userId, String password) throws ConnectionException {
    return executeWithRetry(() -> delegate.setPassword(userId, password),
                            "The setPassword method returned a null result.");
  }

  @Override
  public ResetPasswordResult resetPassword(String userId) throws ConnectionException {
    return executeWithRetry(() -> delegate.resetPassword(userId),
                            "The resetPassword method returned a null result.");
  }

  @Override
  public DescribeSoftphoneLayoutResult describeSoftphoneLayout() throws ConnectionException {
    return executeWithRetry(delegate::describeSoftphoneLayout,
                            "The describeSoftphoneLayout method returned a null result.");
  }

  @Override
  public SaveResult[] create(SObject[] sObjects) throws ConnectionException {
    return executeWithRetry(() -> delegate.create(sObjects),
                            "The create method returned a null result.");
  }

  @Override
  public DescribeSearchLayoutResult[] describeSearchLayouts(String[] sObjectType) throws ConnectionException {
    return executeWithRetry(() -> delegate.describeSearchLayouts(sObjectType),
                            "The describeSearchLayouts method returned a null result.");
  }

  @Override
  public MergeResult[] merge(MergeRequest[] request) throws ConnectionException {
    return executeWithRetry(() -> delegate.merge(request),
                            "The merge method returned a null result.");
  }

  @Override
  public InvalidateSessionsResult[] invalidateSessions(String[] sessionIds) throws ConnectionException {
    return executeWithRetry(() -> delegate.invalidateSessions(sessionIds),
                            "The invalidateSessions method returned a null result.");
  }

  @Override
  public DescribeListViewResult[] describeListViews(String[] sObjectType) throws ConnectionException {
    return executeWithRetry(() -> delegate.describeListViews(sObjectType),
                            "The describeListViews method returned a null result.");
  }

  @Override
  public DescribeDataCategoryMappingResult[] describeDataCategoryMappings() throws ConnectionException {
    return executeWithRetry(delegate::describeDataCategoryMappings,
                            "The describeDataCategoryMappings method returned a null result.");
  }

  @Override
  public void logout() throws ConnectionException {
    delegate.logout();
  }

  @Override
  public DescribeSearchScopeOrderResult[] describeSearchScopeOrder(boolean includeRealTimeEntities) throws
    ConnectionException {
    return executeWithRetry(() -> delegate.describeSearchScopeOrder(includeRealTimeEntities),
                            "The describeSearchScopeOrder method returned a null result.");
  }

  /**
   * Executes a Salesforce PartnerConnection operation with retry logic using the provided {@link RetryPolicy}.
   * <p>
   * This method wraps the execution in {@link Failsafe} to handle transient {@link ConnectionException}s
   * and retries the operation if the exception is considered retryable.
   * It also performs a null or custom result validation check, throwing an {@link IllegalArgumentException}
   * if the result is invalid.
   *
   * @param operation    the Salesforce PartnerConnection operation to execute
   * @param errorContext a descriptive message used when the result is null or fails the validation check
   * @param <T>          the type of the result returned by the operation
   * @return the result of the successful operation
   * @throws ConnectionException      if the operation fails with a non-retryable {@link ConnectionException}
   * @throws IllegalArgumentException if the result is null or fails the provided validation check
   */
  private <T> T executeWithRetry(Callable<T> operation, String errorContext) throws ConnectionException {
    try {
      return Failsafe.with(retryPolicy).get(() -> {
        try {
          T result = operation.call();
          if (result == null) {
            throw new IllegalArgumentException(errorContext);
          }
          return result;
        } catch (ConnectionException e) {
          if (isRetryableConnectionError(e)) {
            throw new SalesforceQueryExecutionException(e);
          }
          throw e;
        }
      });
    } catch (FailsafeException ex) {
      if (ex.getCause() instanceof ConnectionException) {
        throw (ConnectionException) ex.getCause();
      }
      throw ex;
    }
  }

  public boolean isRetryableConnectionError(ConnectionException e) {
    if (e.getCause() instanceof SocketTimeoutException) {
      return true;
    }
    if (e.getCause() instanceof ApiFault) {
      ApiFault apifault = (ApiFault) e.getCause();
      return RETRYABLE_API_FAULT_ERRORS.contains(apifault.getExceptionCode().toString());
    }
    String error = e.getMessage();
    if (error == null) {
      return false;
    }
    error = error.toLowerCase();
    return RETRYABLE_CONNECTION_ERRORS.stream()
      .anyMatch(error::contains);
  }
}
