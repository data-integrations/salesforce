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
  // https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/
  // async_api_headers_enable_pk_chunking.htm
  public static final int MAX_PK_CHUNK_SIZE = 250000;
  public static final int DEFAULT_PK_CHUNK_SIZE = 100000;
  public static final int MIN_PK_CHUNK_SIZE = 1;
  // https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/
  // async_api_headers_enable_pk_chunking.htm
  public static final List<String> SUPPORTED_OBJECTS_WITH_PK_CHUNK = Arrays.asList("Account",
                                                                                   "AccountContactRelation",
                                                                                   "AccountTeamMember",
                                                                                   "AiVisitSummary",
                                                                                   "Asset",
                                                                                   "B2BMktActivity",
                                                                                   "B2BMktProspect",
                                                                                   "Campaign",
                                                                                   "CampaignMember",
                                                                                   "CandidateAnswer",
                                                                                   "Case",
                                                                                   "CaseArticle",
                                                                                   "CaseComment",
                                                                                   "Claim",
                                                                                   "ClaimParticipant",
                                                                                   "Contact",
                                                                                   "ContractLineItem",
                                                                                   "ConversationEntry",
                                                                                   "CustomerProperty",
                                                                                   "EinsteinAnswerFeedback",
                                                                                   "EmailMessage",
                                                                                   "EngagementScore",
                                                                                   "Event",
                                                                                   "EventRelation",
                                                                                   "FeedItem",
                                                                                   "Individual",
                                                                                   "InsurancePolicy",
                                                                                   "InsurancePolicyAsset",
                                                                                   "InsurancePolicyParticipant",
                                                                                   "Lead",
                                                                                   "LeadInsight",
                                                                                   "LiveChatTranscript",
                                                                                   "LoginHistory",
                                                                                   "LoyaltyLedger",
                                                                                   "LoyaltyMemberCurrency",
                                                                                   "LoyaltyMemberTier",
                                                                                   "LoyaltyPartnerProduct",
                                                                                   "LoyaltyProgramMember",
                                                                                   "LoyaltyProgramPartner",
                                                                                   "Note",
                                                                                   "ObjectTerritory2Association",
                                                                                   "Opportunity",
                                                                                   "OpportunityContactRole",
                                                                                   "OpportunityHistory",
                                                                                   "OpportunityLineItem",
                                                                                   "OpportunitySplit",
                                                                                   "OpportunityTeamMember",
                                                                                   "Pricebook2",
                                                                                   "PricebookEntry",
                                                                                   "Product2",
                                                                                   "ProductConsumed",
                                                                                   "ProductRequired",
                                                                                   "QuickText",
                                                                                   "Quote",
                                                                                   "QuoteLineItem",
                                                                                   "ReplyText",
                                                                                   "ScoreIntelligence",
                                                                                   "ServiceContract",
                                                                                   "Task",
                                                                                   "TermDocumentFrequency",
                                                                                   "TransactionJournal",
                                                                                   "User",
                                                                                   "UserRole",
                                                                                   "VoiceCall",
                                                                                   "WorkOrder",
                                                                                   "WorkOrderLineItem");

  /**
   * Salesforce Bulk API has a limitation, which is 10 minutes per processing of a batch
   */
  public static final long GET_BATCH_WAIT_TIME_SECONDS = 600;
  /**
   * Sleep time between polling the batch status
   */
  public static final long GET_BATCH_RESULTS_SLEEP_MS = 500;
  /**
   * Number of tries while polling the batch status
   */
  public static final long GET_BATCH_RESULTS_TRIES = GET_BATCH_WAIT_TIME_SECONDS * (1000 / GET_BATCH_RESULTS_SLEEP_MS);
}
