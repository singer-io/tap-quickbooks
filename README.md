# tap-quickbooks

**This tap is in development.**

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

## Description
This tap:
- Pulls raw data from [Quickbooks API](https://developer.intuit.com/app/developer/qbo/docs/develop)
- Extracts from the following sources to produce [streams](https://github.com/singer-io/tap-quickbooks/blob/master/tap_quickbooks/streams.py). Below is a list of all the streams available. See the [streams file](https://github.com/singer-io/tap-quickbooks/blob/master/tap_quickbooks/streams.py) for a list of classes where each one has a constant indiciating if the stream's replication_method is INCREMENTAL or FULL_TABLE and what is the replication_key.
    * Stream
    * Accounts
    * Budgets
    * Classes
    * CreditMemos
    * BillPayments
    * SalesReceipts
    * Purchases
    * Payments
    * PurchaseOrders
    * PaymentMethods
    * JournalEntries
    * Items
    * Invoices
    * Customers
    * RefundReceipts
    * Deposits
    * Departments
    * Employees
    * Estimates
    * Bills
    * TaxAgencies
    * TaxCodes
    * TaxRates
    * Terms
    * TimeActivities
    * Transfers
    * VendorCredits
    * Vendors
    * ProfitAndLossReport
    * DeletedObjects

- Includes a schema for each resource reflecting most recent tested data retrieved using the api. See [the schema folder](https://github.com/singer-io/tap-quickbooks/tree/master/tap_quickbooks/schemas) for details.
- Incrementally pulls data based on the input state

## Authentication

Authentication is handled with oauth v2. In the tap configuration the following fields are required for authentication to work correctly:

* client_id
* client_secret
* refresh_token

These values are all obtained from the oauth steps documented on [quickbook's documentation page](https://developer.intuit.com/app/developer/qbo/docs/develop/authentication-and-authorization/oauth-2.0#obtain-the-access-token).

## Quick Start

1. Install

    Clone this repository, and then install using setup.py. We recommend using a virtualenv:

    ```bash
    $ virtualenv -p python3 venv
    $ source venv/bin/activate
    $ pip install -e .
    ```
1. Create your tap's `config.json` file.  The tap config file for this tap should include these entries:
   - `start_date` - The default value to use if no bookmark exists for an endpoint (rfc3339 date string)
   - `user_agent` (string): Process and email for API logging purposes. Example: `tap-quickbooks <api_user_email@your_company.com>`
   - `realm_id` (string): The realm id of the company to fetch the data from.
   - `client_secret` (string): Credentials of the client app.
   - `client_id` (string): Id of the client app.
   - `refresh_token` (string): Token to get a new Access token if it expires.
   - `sandbox` (string, optional): Whether to communicate with quickbooks's sandbox or prod account for this application. If you're not sure leave out. Defaults to false.
   - The `request_timeout` is an optional paramater to set timeout for requests. Default: 300 seconds

   And the other values mentioned in [the authentication section above](#authentication).

    ```json
	{
		"client_id": "<app_id>",
		"start_date": "2020-08-21T00:00:00Z",
		"refresh_token": "<refresh_token>",
		"client_secret": "<app_secret>",
		"realm_id": "0123456789",
		"sandbox": "<true|false>",
		"user_agent": "Stitch Tap (+support@stitchdata.com)",
		"request_timeout": 300
	}
	```

1. Run the Tap in Discovery Mode
    This creates a catalog.json for selecting objects/fields to integrate:
    ```bash
    tap-quickbooks --config config.json --discover > catalog.json
    ```
   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

5. Run the Tap in Sync Mode (with catalog) and [write out to state file](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap-with-a-singer-target)

    For Sync mode:
    ```bash
    $ tap-quickbooks --config tap_config.json --catalog catalog.json >> state.json
    $ tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To load to json files to verify outputs:
    ```bash
    $ tap-quickbooks --config tap_config.json --catalog catalog.json | target-json >> state.json
    $ tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To pseudo-load to [Stitch Import API](https://github.com/singer-io/target-stitch) with dry run:
    ```bash
    $ tap-quickbooks --config tap_config.json --catalog catalog.json | target-stitch --config target_config.json --dry-run >> state.json
    $ tail -1 state.json > state.json.tmp && mv state.json.tmp state.json

---

Copyright &copy; 2020 Stitch
