// File: salesforce/force-app/main/default/triggers/ChurnScoreTrigger.trigger
/*
 * Trigger on the Churn_Score__x external object (Salesforce Connect).
 *
 * NOTE ON ACTIVATION PATH: Salesforce Connect external objects do not fire
 * Apex triggers natively. This trigger is deployed against the shadow
 * custom object pattern where a sync process (Platform Event consumer or
 * ETL middleware) materializes each score row into a local record; the
 * declarative alternative is documented in salesforce/flows/.
 *
 * All logic lives in the handler class. The trigger body stays thin,
 * bulkification-safe, and gated by a Custom Permission-style feature flag.
 */
trigger ChurnScoreTrigger on Churn_Score__x (after insert, after update) {
    // Runtime kill switch: Feature Management > Disable_Churn_Alerts
    if (System.FeatureManagement.checkValue('Disable_Churn_Alerts')) {
        return;
    }
    new ChurnScoreTriggerHandler(Trigger.new).run();
}
