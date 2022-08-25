from terra_billing_alert.cli import main as cli_main


def main(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    # Reads all configuration from environment variables
    # See https://github.com/IGVF-DACC/terra-billing-alert for details
    cli_main()
    return 'Success'
