# File: salesforce/flows/Churn_Alert_Flow.md
# Churn Alert Flow - Record-Triggered Flow Spec

Declarative alternative to `ChurnScoreTrigger` for orgs that prefer Flow.
Runs on the same activation records (`Churn_Score__x` external object, or a
shadow `Churn_Score__c` custom object populated by sync middleware).

## Trigger

| Setting | Value |
|---|---|
| Type | Record-Triggered |
| Object | Churn Score |
| Events | A record is created or updated |
| Optimized for | Actions and Related Records |

## Entry Conditions (formula, all must be true)

```
{!$Record.Churn_Probability_Score__c} > 0.70
&&
ISPICKVAL({!$Record.Risk_Tier__c}, "HIGH")
```

## Elements

1. **Decision: Account resolvable?**
   - `{!$Record.Account_External_Id__c}` is not null -> continue; else end with
     a scheduled path writing to an error log custom object.
2. **Get Records: Account** — filter `External_Id__c = {!$Record.Account_External_Id__c}`,
   store `OwnerId`.
3. **Create Records: AE Save-Play Task** (no bulk loop needed; Flow fires per record)
   - Subject: `High churn risk - action required`
   - Assigned To: `{!Get_Account.OwnerId}`
   - Related To: `{!Get_Account.Id}`
   - Priority: High; Due Date: today + 2 days.
4. **Send Platform Event** — `Churn_Alert__e`
   - Map `Customer_Key__c`, `Score__c`, `Risk_Tier__c`, `Model_Version__c`.

## Fault Handling & Governance

- Default fault path logs failures and does not block the score sync.
- Entry-condition dedupe mirrors the Apex handler's recursion gate: repeated
  updates within 24h are suppressed via a `Last_Alert_At__c` field check
  (`{!$Record.Scored_At__c} > {!$Record.Last_Alert_At__c}`).
- Volume guardrail: if nightly high-risk volume exceeds ~2k/day, switch Task
  creation from per-record to a scheduled path + batched create.

## Deployment Notes

- Activate AFTER the Salesforce Connect datasource `Customer360_OData` exists.
- Keep exactly one of {trigger path, flow path} active per environment;
  both honor the `Disable_Churn_Alerts` feature flag where supported.
