/*
 * Copyright © 2019 Cask Data, Inc.
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
package io.cdap.plugin.salesforce.plugin.source.batch.util;

import java.util.Arrays;
import java.util.List;

/**
 * Salesforce batch source constants
 */
public class SalesforceSourceConstants {

  public static final String PROPERTY_DATETIME_AFTER = "datetimeAfter";
  public static final String PROPERTY_DATETIME_BEFORE = "datetimeBefore";
  public static final String PROPERTY_DURATION = "duration";
  public static final String PROPERTY_OFFSET = "offset";
  public static final String PROPERTY_SCHEMA = "schema";

  public static final String PROPERTY_QUERY = "query";
  public static final String PROPERTY_SOBJECT_NAME = "sObjectName";
  public static final String PROPERTY_OPERATION = "operation";
  public static final String PROPERTY_PK_CHUNK_ENABLE_NAME = "enablePKChunk";
  public static final String PROPERTY_CHUNK_SIZE_NAME = "chunkSize";
  public static final String PROPERTY_PARENT_NAME = "parent";

  public static final String PROPERTY_WHITE_LIST = "whiteList";
  public static final String PROPERTY_BLACK_LIST = "blackList";
  public static final String PROPERTY_SOBJECT_NAME_FIELD = "sObjectNameField";

  public static final String CONFIG_SCHEMAS = "mapred.salesforce.input.schemas";
  public static final String CONFIG_QUERY_SPLITS = "mapred.salesforce.input.query.splits";

  public static final String HEADER_ENABLE_PK_CHUNK = "Sforce-Enable-PKChunking";
  public static final String HEADER_VALUE_PK_CHUNK = "chunkSize=%d";
  public static final String HEADER_PK_CHUNK_PARENT = "parent=%s";

  public static final String CONFIG_SOBJECT_NAME_FIELD = "mapred.salesforce.input.sObjectNameField";
  public static final int WIDE_QUERY_MAX_BATCH_COUNT = 2000;
  public static final int RETRIEVE_MAX_BATCH_COUNT = 2000;
  // https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/
  // async_api_headers_enable_pk_chunking.htm
  public static final int MAX_PK_CHUNK_SIZE = 250000;
  public static final int DEFAULT_PK_CHUNK_SIZE = 100000;
  public static final int MIN_PK_CHUNK_SIZE = 1;
  // https://developer.salesforce.com/docs/atlas.en-us.252.0.api_asynch.meta/api_asynch/
  // async_api_headers_enable_pk_chunking.htm
  public static final List<String> SUPPORTED_OBJECTS_WITH_PK_CHUNK = Arrays.asList("account",
                                                                                   "accountcontactrelation",
                                                                                   "accountteammember",
                                                                                   "aivisitsummary",
                                                                                   "asset",
                                                                                   "assignedresource",
                                                                                   "campaign",
                                                                                   "campaignmember",
                                                                                   "candidateanswer",
                                                                                   "case",
                                                                                   "casearticle",
                                                                                   "casecomment",
                                                                                   "caserelatedissue",
                                                                                   "changerequest",
                                                                                   "changerequestrelatedissue",
                                                                                   "changerequestrelateditem",
                                                                                   "claim",
                                                                                   "claimparticipant",
                                                                                   "contact",
                                                                                   "contentdistribution",
                                                                                   "contentdocument",
                                                                                   "contentnote",
                                                                                   "contentversion",
                                                                                   "contract",
                                                                                   "contractlineitem",
                                                                                   "conversationdefinitioneventlog",
                                                                                   "conversationentry",
                                                                                   "conversationreason",
                                                                                   "conversationreasonexcerpt",
                                                                                   "conversationreasongroup",
                                                                                   "customerproperty",
                                                                                   "einsteinanswerfeedback",
                                                                                   "emailmessage",
                                                                                   "engagementscore",
                                                                                   "entitlement",
                                                                                   "event",
                                                                                   "eventrelation",
                                                                                   "feeditem",
                                                                                   "incident",
                                                                                   "incidentrelateditem",
                                                                                   "individual",
                                                                                   "insurancepolicy",
                                                                                   "insurancepolicyasset",
                                                                                   "insurancepolicyparticipant",
                                                                                   "lead",
                                                                                   "leadinsight",
                                                                                   "linkedarticle",
                                                                                   "livechattranscript",
                                                                                   "loginhistory",
                                                                                   "loyaltyaggrpointexprledger",
                                                                                   "loyaltyledger",
                                                                                   "loyaltymembercurrency",
                                                                                   "loyaltymembertier",
                                                                                   "loyaltypartnerproduct",
                                                                                   "loyaltyprogrammbrpromotion",
                                                                                   "loyaltyprogrammember",
                                                                                   "loyaltyprogrampartner",
                                                                                   "loyaltyprogrampartnerledger",
                                                                                   "messagingsession",
                                                                                   "mlretrainingfeedback",
                                                                                   "note",
                                                                                   "objectterritory2association",
                                                                                   "opportunity",
                                                                                   "opportunitycontactrole",
                                                                                   "opportunityhistory",
                                                                                   "opportunitylineitem",
                                                                                   "opportunitysplit",
                                                                                   "opportunityteammember",
                                                                                   "order",
                                                                                   "orderitem",
                                                                                   "pricebook2",
                                                                                   "pricebookentry",
                                                                                   "problem",
                                                                                   "problemincident",
                                                                                   "problemrelateditem",
                                                                                   "product2",
                                                                                   "productconsumed",
                                                                                   "productrequired",
                                                                                   "quicktext",
                                                                                   "quote",
                                                                                   "quotelineitem",
                                                                                   "replytext",
                                                                                   "scoreintelligence",
                                                                                   "serviceappointment",
                                                                                   "servicecontract",
                                                                                   "task",
                                                                                   "taskrelation",
                                                                                   "termdocumentfrequency",
                                                                                   "timesheetentry",
                                                                                   "transactionjournal",
                                                                                   "user",
                                                                                   "userrole",
                                                                                   "voicecall",
                                                                                   "voicecallrecording",
                                                                                   "voucher",
                                                                                   "webcart",
                                                                                   "workloadunit",
                                                                                   "workorder",
                                                                                   "workorderlineitem",
                                                                                   "workplan",
                                                                                   "workplantemplate");

  /**
   * Salesforce Bulk API has a limitation, which is 5 minutes per processing of a batch, after that it will
   * retry 20 times before failing the batch. Refer below link.
   * https://developer.salesforce.com/docs/atlas.en-us.242.0.salesforce_app_limits_cheatsheet.meta/
   * salesforce_app_limits_cheatsheet/salesforce_app_limits_platform_bulkapi.htm
   */
  public static final long GET_BATCH_WAIT_TIME_SECONDS = 6000;
  /**
   * Sleep time between polling the batch status
   */
  public static final long GET_BATCH_RESULTS_SLEEP_MS = 5000;

  /**
   * Sleep time between polling the batch status for Salesforce Sink
   */
  public static final long GET_SINK_BATCH_RESULTS_SLEEP_MS = 5000;

  /**
   * Number of tries while polling the batch status
   */
  public static final long GET_BATCH_RESULTS_TRIES = (long) (GET_BATCH_WAIT_TIME_SECONDS *
    (1000 * 1.0 / GET_BATCH_RESULTS_SLEEP_MS));

  /**
   * Salesforce Bulk API has a limitation, which is 10 minutes per processing of a batch. But for serial mode,
   * we can't get the exact no. of batches as batches are getting submitted from multiple nodes.
   * So, setting this to a large value to avoid timeout issues.
   */
  public static final long GET_BATCH_WAIT_TIME_SECONDS_SERIAL_MODE = 600000;

  public static final long MAX_RETRIES_ON_API_FAILURE = 10;

}
