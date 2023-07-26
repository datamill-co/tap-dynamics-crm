from singer.catalog import Catalog, CatalogEntry, Schema

selected_tables = [
    "accounts",
    "leads",
    "opportunities",
    "contacts",
    "transactioncurrencies",
    "salesorders",
    "systemusers",
    "msdyncrm_linkedinformsubmissions",
    "activitypointers",
]


def get_schema(odata_schema):
    json_props = {}
    metadata = []
    pks = []
    for odata_prop in odata_schema.get("properties", []):
        odata_type = odata_prop["type"]
        prop_name = odata_prop["name"]
        json_type = "string"
        json_format = None

        inclusion = "automatic"
        if odata_prop["is_primary_key"] == True:
            pks.append(prop_name)

        metadata.append(
            {
                "breadcrumb": ["properties", prop_name],
                "metadata": {"inclusion": inclusion},
            }
        )

        if odata_type in ["Edm.Date", "Edm.DateTime", "Edm.DateTimeOffset"]:
            json_format = "date-time"
        elif odata_type in ["Edm.Int16", "Edm.Int32", "Edm.Int64"]:
            json_type = "integer"
        elif odata_type in ["Edm.Double", "Edm.Decimal"]:
            json_type = "number"
        elif odata_type == "Edm.Boolean":
            json_type = "boolean"

        prop_json_schema = {"type": ["null", json_type]}

        if json_format:
            prop_json_schema["format"] = json_format

        json_props[prop_name] = prop_json_schema

    json_schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": json_props,
    }

    return json_schema, metadata, pks


def discover(service):
    catalog = Catalog([])

    for entity_name, entity in service.entities.items():
        if entity_name not in selected_tables:
            continue
        schema_dict, metadata, pks = get_schema(entity.__odata_schema__)
        metadata.append({"breadcrumb": [], "metadata": {"selected": True}})
        schema = Schema.from_dict(schema_dict)

        catalog.streams.append(
            CatalogEntry(
                stream=entity_name,
                tap_stream_id=entity_name,
                key_properties=pks,
                schema=schema,
                metadata=metadata,
                replication_method="INCREMENTAL"
                if schema_dict.get("properties", None).get("createdon", None)
                else "FULL_TABLE",
            )
        )

    return catalog
