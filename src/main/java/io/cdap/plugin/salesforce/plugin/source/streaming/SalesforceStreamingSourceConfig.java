/*
 * Copyright 2019 Google Inc. All Rights Reserved.
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

package io.cdap.plugin.salesforce.plugin.source.streaming;

import com.google.common.base.Strings;
import com.sforce.soap.partner.FieldType;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.InvalidStageException;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.common.ReferencePluginConfig;
import io.cdap.plugin.salesforce.InvalidConfigException;
import io.cdap.plugin.salesforce.SObjectDescriptor;
import io.cdap.plugin.salesforce.SObjectFilterDescriptor;
import io.cdap.plugin.salesforce.SalesforceConnectionUtil;
import io.cdap.plugin.salesforce.SalesforceConstants;
import io.cdap.plugin.salesforce.SalesforceQueryUtil;
import io.cdap.plugin.salesforce.authenticator.Authenticator;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.salesforce.plugin.OAuthInfo;
import io.cdap.plugin.salesforce.plugin.SalesforceConnectorBaseConfig;
import io.cdap.plugin.salesforce.plugin.SalesforceConnectorInfo;
import io.cdap.plugin.salesforce.plugin.connector.SalesforceConnectorConfig;
import io.cdap.plugin.salesforce.plugin.source.batch.util.SalesforceSourceConstants;
import io.cdap.plugin.salesforce.soap.SObjectBuilder;
import io.cdap.plugin.salesforce.soap.SObjectUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Salesforce Streaming Source plugin config.
 */
public class SalesforceStreamingSourceConfig extends ReferencePluginConfig implements Serializable {
  protected static final String ENABLED_KEYWORD = "Enabled";
  protected static final String PROPERTY_PUSH_TOPIC_NAME = "pushTopicName";
  protected static final String PROPERTY_PUSH_TOPIC_QUERY = "pushTopicQuery";
  protected static final String PROPERTY_SOBJECT_NAME = "sObjectName";
  private static final Logger LOG = LoggerFactory.getLogger(SalesforceStreamingSourceConfig.class);
  private static final long serialVersionUID = 4218063781902315444L;
  private static final Pattern isValidFieldNamePattern = Pattern.compile("[a-zA-Z0-9.-_]+");
  @Description("Salesforce push topic name. Plugin will track updates from this topic. If topic does not exist, " +
    "it will be automatically created. " +
    "To manually create pushTopic use Salesforce workbench or Apex code or API.")
  @Name(PROPERTY_PUSH_TOPIC_NAME)
  @Macro
  private String pushTopicName;

  @Description("Salesforce push topic query. The query is used by Salesforce to send updates to push topic. " +
    "This field not required, if you are using an existing push topic.")
  @Nullable
  @Name(PROPERTY_PUSH_TOPIC_QUERY)
  @Macro
  private String pushTopicQuery;

  @Description("Push topic property, which specifies if a create operation should generate a record.")
  @Name("pushTopicNotifyCreate")
  private String pushTopicNotifyCreate;

  @Description("Push topic property, which specifies if a update operation should generate a record.")
  @Name("pushTopicNotifyUpdate")
  private String pushTopicNotifyUpdate;

  @Description("Push topic property, which specifies if an delete operation should generate a record.")
  @Name("pushTopicNotifyDelete")
  private String pushTopicNotifyDelete;

  @Description("Salesforce SObject name used to automatically generate query. Example: Opportunity.")
  @Nullable
  @Name(PROPERTY_SOBJECT_NAME)
  @Macro
  private String sObjectName;

  @Name(ConfigUtil.NAME_USE_CONNECTION)
  @Nullable
  @Description("Whether to use an existing connection.")
  private Boolean useConnection;

  @Name(ConfigUtil.NAME_CONNECTION)
  @Macro
  @Nullable
  @Description("The existing connection to use.")
  private SalesforceConnectorBaseConfig connection;

  @Name(SalesforceConstants.PROPERTY_OAUTH_INFO)
  @Description("OAuth information for connecting to Salesforce. " +
    "It is expected to be an json string containing two properties, \"accessToken\" and \"instanceURL\", " +
    "which carry the OAuth access token and the URL to connect to respectively. " +
    "Use the ${oauth(provider, credentialId)} macro function for acquiring OAuth information dynamically. ")
  @Macro
  @Nullable
  private OAuthInfo oAuthInfo;

  @Description("Push topic property, which specifies how the record is evaluated against the PushTopic query.\n" +
    "The NotifyForFields values are:\n" +
    "All - Notifications are generated for all record field changes, provided the evaluated records match " +
    "the criteria specified in the WHERE clause.\n" +
    "Referenced (default) - Changes to fields referenced in the SELECT and WHERE clauses are evaluated. " +
    "Notifications are generated for the evaluated " +
    "records only if they match the criteria specified in the WHERE clause.\n" +
    "Select - Changes to fields referenced in the SELECT clause are evaluated. Notifications are generated " +
    "for the evaluated records only if they match the criteria specified in the WHERE clause.\n" +
    "Where - Changes to fields referenced in the WHERE clause are evaluated. Notifications are generated " +
    "for the evaluated records only if they match the criteria specified in the WHERE clause.")
  @Name("pushTopicNotifyForFields")
  private String pushTopicNotifyForFields;

  @Name(SalesforceSourceConstants.PROPERTY_SCHEMA)
  @Macro
  @Nullable
  @Description("Schema of the data to read. Can be imported or fetched by clicking the `Get Schema` button.")
  private String schema;

  public SalesforceStreamingSourceConfig(String referenceName,
                                         @Nullable String consumerKey,
                                         @Nullable String consumerSecret,
                                         @Nullable String username,
                                         @Nullable String password,
                                         @Nullable String loginUrl,
                                         String pushTopicName, String sObjectName,
                                         @Nullable String securityToken,
                                         @Nullable Integer connectTimeout,
                                         @Nullable Integer readTimeout,
                                         @Nullable OAuthInfo oAuthInfo,
                                         @Nullable String proxyUrl,
                                         @Nullable String schema) {
    super(referenceName);
    this.pushTopicName = pushTopicName;
    this.sObjectName = sObjectName;
    this.connection = new SalesforceConnectorConfig(consumerKey, consumerSecret, username, password, loginUrl,
                                                    securityToken, connectTimeout, readTimeout, oAuthInfo, proxyUrl);
    this.schema = schema;
  }

  @Nullable
  public SalesforceConnectorInfo getConnection() {
    return connection == null ? null : new SalesforceConnectorInfo(oAuthInfo, connection,
                                                                   SalesforceConstants.isOAuthMacroFunction.apply(
                                                                     this));
  }

  public String getPushTopicName() {
    return pushTopicName;
  }

  public String getPushTopicQuery() {
    return pushTopicQuery;
  }

  public Boolean isPushTopicNotifyCreate() {
    return pushTopicNotifyCreate.equals(ENABLED_KEYWORD);
  }

  public Boolean isPushTopicNotifyUpdate() {
    return pushTopicNotifyUpdate.equals(ENABLED_KEYWORD);
  }

  public Boolean isPushTopicNotifyDelete() {
    return pushTopicNotifyDelete.equals(ENABLED_KEYWORD);
  }

  public String getPushTopicNotifyForFields() {
    return pushTopicNotifyForFields;
  }

  @Nullable
  public Schema getSchema() {
    try {
      return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
    } catch (IOException e) {
      throw new InvalidConfigException("Unable to parse output schema: " +
                                         schema, e, SalesforceSourceConstants.PROPERTY_SCHEMA);
    }
  }

  /**
   * Get query to use, either pushTopicQuery or query generated using sObjectName.
   *
   * @return query or null if query was not specified via pushTopicQuery and sObjectName.
   */
  @Nullable
  public String getQuery() {
    if (!Strings.isNullOrEmpty(pushTopicQuery)) {
      return pushTopicQuery;
    } else if (!Strings.isNullOrEmpty(sObjectName)) {
      return getSObjectQuery();
    } else {
      // If both are empty. Plugin will still work if pushTopic already exists.
      return null;
    }
  }

  /**
   * Asserts that pushTopic on Salesforce server has the same parameters as specified in config.
   * If they are different {@link java.lang.IllegalArgumentException} is thrown.
   * <p>
   * If pushTopic does not exist it is created.
   */
  public void ensurePushTopicExistAndWithCorrectFields(OAuthInfo oAuthInfo) {
    if (connection != null) {
      if (containsMacro(PROPERTY_PUSH_TOPIC_NAME) ||
        containsMacro(PROPERTY_PUSH_TOPIC_QUERY) || oAuthInfo == null) {
        return;
      }
    }

    try {
      PartnerConnection partnerConnection = new PartnerConnection(
        Authenticator.createConnectorConfig(new AuthenticatorCredentials(oAuthInfo,
                                                                         this.getConnection().getConnectTimeout(),
                                                                         this.getConnection().getReadTimeout(),
                                                                         this.connection.getProxyUrl())));

      SObject pushTopic = fetchPushTopicByName(partnerConnection, pushTopicName);
      String query = getQuery();

      if (pushTopic == null) {
        LOG.info("Creating PushTopic {}", pushTopicName);

        if (Strings.isNullOrEmpty(query)) {
          throw new InvalidConfigException("SOQL query or SObject name must be provided, unless " +
                                             "existing pushTopic is used",
                                           SalesforceStreamingSourceConfig.PROPERTY_PUSH_TOPIC_QUERY);
        }

        pushTopic = new SObjectBuilder()
          .setType("PushTopic")
          .put("Name", pushTopicName)
          .put("Query", query)
          .put("NotifyForOperationCreate", isPushTopicNotifyCreate().toString())
          .put("NotifyForOperationUpdate", isPushTopicNotifyUpdate().toString())
          .put("NotifyForOperationDelete", isPushTopicNotifyDelete().toString())
          .put("NotifyForFields", getPushTopicNotifyForFields())
          .put("ApiVersion", SalesforceConstants.API_VERSION)
          .build();

        SObjectUtil.createSObjects(partnerConnection, new SObject[]{pushTopic});
      } else {
        if (!Strings.isNullOrEmpty(query)) {
          assertFieldValue(pushTopic, "Query", query);
        } else {
          pushTopicQuery = (String) pushTopic.getField("Query");
        }

        // Ensure that pushTopic has the same parameters as user set in config.
        // Otherwise it would be confusing for user if we continue
        assertFieldValue(pushTopic, "NotifyForOperationCreate", isPushTopicNotifyCreate().toString());
        assertFieldValue(pushTopic, "NotifyForOperationUpdate", isPushTopicNotifyUpdate().toString());
        assertFieldValue(pushTopic, "NotifyForOperationDelete", isPushTopicNotifyDelete().toString());
        assertFieldValue(pushTopic, "NotifyForFields", getPushTopicNotifyForFields());
      }
    } catch (ConnectionException e) {
      String message = SalesforceConnectionUtil.getSalesforceErrorMessageFromException(e);
      throw new InvalidStageException(
        String.format("Cannot connect to Salesforce API with credentials specified due to error: %s", message), e);
    }
  }

  /**
   * Returns pushTopic with given name from Salesforce server if present.
   *
   * @param partnerConnection a connection to Salesforce API
   * @param pushTopicName a name of the push topic
   * @return returns push topic or {@code null} if not found
   * @throws ConnectionException occurs due to failure to connect to Salesforce API
   */
  public static SObject fetchPushTopicByName(PartnerConnection partnerConnection, String pushTopicName)
    throws ConnectionException {
    // in case somebody attempts SOQL injection
    if (!isValidFieldName(pushTopicName)) {
      throw new IllegalArgumentException(String.format(
        "Push topic name '%s' can only contain latin letters.", pushTopicName));
    }

    QueryResult queryResult =
      runQuery(partnerConnection, String.format("SELECT Id, Name, Query, NotifyForOperationCreate, " +
                                                  "NotifyForOperationUpdate, NotifyForOperationDelete, " +
                                                  "NotifyForFields FROM PushTopic WHERE Name = '%s'", pushTopicName));


    SObject[] records = queryResult.getRecords();

    switch(records.length) {
      case 0:
        return null;
      case 1:
        return records[0];
      default:
        throw new IllegalStateException(String.format("Excepted one or zero pushTopics with name = '%s' found %d",
                                                      pushTopicName, records.length));
    }
  }

  /**
   * For integration tests it will wrap the actual SOQL query call with change of classloaders,
   * for production will do nothing else but run query.
   *
   * Some context:
   * Salesforce query method uses isAssignableFrom to check if received data is of correct class.
   * However the fact that in integration tests we use multiple different classloaders, makes Salesforce
   * think that there is a value of wrong class supplied, while in reality the class is the same but loaded with
   * different instance of class loader.
   * Unfortunately this behavior cannot be mocked, due to need to mock classes in non-main class loader.
   *
   * @param query a SOQL query
   * @return query result
   */
  public static QueryResult runQuery(PartnerConnection partnerConnection, String query) throws ConnectionException {
    ClassLoader threadClassLoader = Thread.currentThread().getContextClassLoader();
    ClassLoader classClassLoader = SalesforceStreamingSourceConfig.class.getClassLoader();

    // will always be false for production runs and true for integration tests
    boolean usesDifferentClassLoaders = !threadClassLoader.equals(classClassLoader);

    try {
      if (usesDifferentClassLoaders) {
        Thread.currentThread().setContextClassLoader(classClassLoader);
      }
      return partnerConnection.query(query);
    } finally {
      if (usesDifferentClassLoaders) {
        Thread.currentThread().setContextClassLoader(threadClassLoader);
      }
    }
  }

  /**
   * Supported character set for fields by Salesforce:
   * A-Z, a-z, 0-9, '.', '-' And '_'
   *
   * @param name fieldName
   * @return true fieldName is valid
   */
  private static boolean isValidFieldName(String name) {
    Matcher m = isValidFieldNamePattern.matcher(name);
    return m.matches();
  }

  private static void assertFieldValue(SObject pushTopic, String fieldName, Object expectedResult) {
    Object actual = pushTopic.getField(fieldName);
    if (!expectedResult.equals(actual)) {
      throw new IllegalArgumentException(
        String.format("Push topic field %s='%s', but existing value on server is '%s'",
                      fieldName, expectedResult, actual));
    }
  }

  @Nullable
  private String getSObjectQuery() {
    SalesforceConnectorInfo connection = getConnection();
    if (!connection.canAttemptToEstablishConnection()) {
      return null;
    }

    try {
      // Text areas are not supported in streaming api queries that's why we are skipping them.
      // Streaming API would respond with "large text area fields are not supported"
      Set<FieldType> typesToSkip = Collections.singleton(FieldType.textarea);
      SObjectDescriptor sObjectDescriptor = SObjectDescriptor.fromName(sObjectName,
                                                                       connection.getAuthenticatorCredentials(),
                                                                       typesToSkip);

      String sObjectQuery = SalesforceQueryUtil.createSObjectQuery(sObjectDescriptor.getFieldsNames(), sObjectName,
                                                                   SObjectFilterDescriptor.noOp());

      LOG.debug("Generated SObject query: '{}'", sObjectQuery);
      return sObjectQuery;
    } catch (ConnectionException e) {
      String message = SalesforceConnectionUtil.getSalesforceErrorMessageFromException(e);
      throw new IllegalStateException(
        String.format("Cannot establish connection to Salesforce to describe SObject: '%s' with error: %s",
                      sObjectName, message), e);
    }
  }
}
