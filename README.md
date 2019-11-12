# tap-dynamics-crm

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from [Dynamics 365 CRM OData API](https://docs.microsoft.com/en-us/dynamics365/customerengagement/on-premises/developer/use-microsoft-dynamics-365-web-api)
- Discovers all available resources and outputs the schema for each resource

## Usage

To run `tap-dynamics` with the configuration file, use this command:

```sh
tap-dynamics -c config.json
```

## Example config

```json
{
  "start_date": "2017-09-10",
  "domain": "dreamdata.crm4",
  "client_id": "<client_id>",
  "client_secret": "<client_secret>",
  "redirect_uri": "<redirect_url>",
  "refresh_token": "<refresh_token>"
}
```
