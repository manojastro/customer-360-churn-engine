// File: salesforce/force-app/main/default/classes/ChurnScoreTriggerHandler.cls
/*
 * Handler for ChurnScoreTrigger.
 *
 * Responsibilities:
 *   - Gate execution (feature flag handled in trigger; recursion gate here).
 *   - Filter scores above the high-risk threshold (default 0.70).
 *   - Resolve Account Executives for the impacted accounts with ONE SOQL.
 *   - Create alert Tasks and publish Churn_Alert__e platform events in bulk:
 *     zero SOQL/DML inside loops, one DML statement per object type.
 */
public without sharing class ChurnScoreTriggerHandler {

    @TestVisible
    private static final Decimal HIGH_RISK_THRESHOLD = 0.70;

    private static final String TASK_SUBJECT = 'High churn risk - action required';
    private static final Integer MAX_ALERTS_PER_TXN = 500;

    /* Records already alerted within this transaction (recursion guard). */
    @TestVisible
    private static final Set<String> processedCustomerKeys = new Set<String>();

    private final List<Churn_Score__x> newRecords;

    public ChurnScoreTriggerHandler(List<Churn_Score__x> newRecords) {
        this.newRecords = newRecords;
    }

    public void run() {
        if (newRecords == null || newRecords.isEmpty()) {
            return;
        }
        List<Churn_Score__x> highRiskScores = filterHighRisk(newRecords);
        if (highRiskScores.isEmpty()) {
            return;
        }
        Map<String, Account> accountByKey = resolveAccounts(highRiskScores);
        List<Task> tasks = buildAlertTasks(highRiskScores, accountByKey);
        List<Churn_Alert__e> events = buildAlertEvents(highRiskScores);
        commit(tasks, events);
    }

    /**
     * Collect records crossing the high-risk threshold, skipping any customer
     * already processed in this transaction and enforcing a batch cap so a
     * mass sync can never blow past governor limits.
     */
    private List<Churn_Score__x> filterHighRisk(List<Churn_Score__x> records) {
        List<Churn_Score__x> highRisk = new List<Churn_Score__x>();
        for (Churn_Score__x score : records) {
            if (score.Churn_Probability_Score__c == null
                    || score.Churn_Probability_Score__c <= HIGH_RISK_THRESHOLD) {
                continue;
            }
            String key = String.isNotBlank(score.Customer_Key__c)
                ? score.Customer_Key__c
                : score.Id;
            if (processedCustomerKeys.contains(key)) {
                continue;
            }
            if (highRisk.size() >= MAX_ALERTS_PER_TXN) {
                System.debug(LoggingLevel.WARN,
                    'ChurnScoreTriggerHandler: alert cap reached, truncating at '
                    + MAX_ALERTS_PER_TXN);
                break;
            }
            processedCustomerKeys.add(key);
            highRisk.add(score);
        }
        return highRisk;
    }

    /** Single SOQL: map external ids to Accounts (with owner) for all keys. */
    private Map<String, Account> resolveAccounts(List<Churn_Score__x> scores) {
        Set<String> externalIds = new Set<String>();
        for (Churn_Score__x score : scores) {
            if (String.isNotBlank(score.Account_External_Id__c)) {
                externalIds.add(score.Account_External_Id__c);
            }
        }
        Map<String, Account> byKey = new Map<String, Account>();
        if (!externalIds.isEmpty()) {
            for (Account acc : [
                SELECT Id, Name, OwnerId
                FROM Account
                WHERE External_Id__c IN :externalIds
            ]) {
                byKey.put(acc.External_Id__c, acc);
            }
        }
        return byKey;
    }

    /** Build one Task per high-risk score, assigned to the owning AE. */
    private List<Task> buildAlertTasks(
        List<Churn_Score__x> scores,
        Map<String, Account> accountByKey
    ) {
        List<Task> tasks = new List<Task>();
        for (Churn_Score__x score : scores) {
            Account acc = accountByKey.get(score.Account_External_Id__c);
            if (acc == null || acc.OwnerId == null) {
                System.debug(LoggingLevel.INFO,
                    'No Account/AE resolved for customer key '
                    + score.Customer_Key__c + '; task skipped');
                continue;
            }
            Task t = new Task(
                Subject = TASK_SUBJECT,
                Description = buildDescription(score),
                WhatId = acc.Id,
                OwnerId = acc.OwnerId,
                Priority = 'High',
                Status = 'Not Started',
                ActivityDate = Date.today().addDays(2)
            );
            tasks.add(t);
        }
        return tasks;
    }

    /** Build platform event payloads for downstream subscribers (Slack, etc.). */
    private List<Churn_Alert__e> buildAlertEvents(List<Churn_Score__x> scores) {
        List<Churn_Alert__e> events = new List<Churn_Alert__e>();
        for (Churn_Score__x score : scores) {
            events.add(new Churn_Alert__e(
                Customer_Key__c = score.Customer_Key__c,
                Score__c = score.Churn_Probability_Score__c,
                Risk_Tier__c = score.Risk_Tier__c,
                Model_Version__c = score.Model_Version__c
            ));
        }
        return events;
    }

    /** Exactly one DML per sObject type, outside of all loops. */
    private void commit(List<Task> tasks, List<Churn_Alert__e> events) {
        if (!tasks.isEmpty()) {
            try {
                insert tasks;
            } catch (DmlException e) {
                // Never block the sync pipeline on activation failures.
                System.debug(LoggingLevel.ERROR,
                    'ChurnScoreTriggerHandler: Task insert failed: ' + e.getMessage());
            }
        }
        if (!events.isEmpty()) {
            try {
                EventBus.publish(events);
            } catch (Exception e) {
                System.debug(LoggingLevel.ERROR,
                    'ChurnScoreTriggerHandler: event publish failed: ' + e.getMessage());
            }
        }
    }

    private String buildDescription(Churn_Score__x score) {
        return 'Predicted churn probability: ' + score.Churn_Probability_Score__c
            + '\nModel version: ' + score.Model_Version__c
            + '\nScored at: ' + score.Scored_At__c
            + '\nOpen a save play before the next renewal checkpoint.';
    }
}
