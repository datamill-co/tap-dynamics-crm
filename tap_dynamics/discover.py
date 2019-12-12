from singer.catalog import Catalog, CatalogEntry, Schema

COLLECTION_LOOKUP_URL = "{}EntityDefinitions?$select=LogicalName&$filter=EntitySetName eq '{}'"
OPTIONSET_METADATA_URL = "{}EntityDefinitions(LogicalName='{}')/Attributes/Microsoft.Dynamics.CRM.PicklistAttributeMetadata?$select=LogicalName&$expand=OptionSet,GlobalOptionSet"
OPTIONSET_MAP = {}

def get_optionset_metadata(service, entity_set_name):
    global OPTIONSET_MAP

    if entity_set_name in OPTIONSET_MAP:
        return OPTIONSET_MAP[entity_set_name]

    entity_optionset_map = {}

    response = service.default_context.connection._do_get(COLLECTION_LOOKUP_URL.format(service.url, entity_set_name))

    if response.status_code == 200:
        data = response.json()['value']
        if data:
            entity_name = data[0]['LogicalName']
            response = service.default_context.connection._do_get(OPTIONSET_METADATA_URL.format(service.url, entity_name))

            if response.status_code != 404:
                response.raise_for_status()
                metadata = response.json()['value']
                for prop in metadata:
                    field_options = {}
                    for option in prop['OptionSet']['Options']:
                         field_options[option['Value']] = option['Label']['UserLocalizedLabel']['Label']
                    for option in prop['GlobalOptionSet']['Options']:
                         field_options[option['Value']] = option['Label']['UserLocalizedLabel']['Label']
                    entity_optionset_map[prop['LogicalName']] = field_options

    OPTIONSET_MAP[entity_set_name] = entity_optionset_map

    return entity_optionset_map

def get_optionset_fieldname(field_name):
    return field_name + '_label'

def get_schema(odata_schema, optionset_map):
    json_props = {}
    metadata = []
    pks = []
    for odata_prop in odata_schema.get('properties', []):
        odata_type = odata_prop['type']
        prop_name = odata_prop['name']
        json_type = 'string'
        json_format = None

        inclusion = 'available'
        if odata_prop['is_primary_key'] == True:
            pks.append(prop_name)
            inclusion = 'automatic'

        metadata.append({
            'breadcrumb': ['properties', prop_name],
            'metadata': {
                'inclusion': inclusion
            }
        })

        if odata_type in ['Edm.Date', 'Edm.DateTime', 'Edm.DateTimeOffset']:
            json_format = 'date-time'
        elif odata_type in ['Edm.Int16', 'Edm.Int32', 'Edm.Int64']:
            json_type = 'integer'
        elif odata_type in ['Edm.Double', 'Edm.Decimal']:
            json_type = 'number'
        elif odata_type == 'Edm.Boolean':
            json_type = 'boolean'

        prop_json_schema = {
            'type': ['null', json_type]
        }

        if json_format:
            prop_json_schema['format'] = json_format

        json_props[prop_name] = prop_json_schema

        if prop_name in optionset_map:
            optionset_fieldname = get_optionset_fieldname(prop_name)
            json_props[optionset_fieldname] = {
                'type': ['null', 'string']
            }
            metadata.append({
                'breadcrumb': ['properties', optionset_fieldname],
                'metadata': {
                    'inclusion': 'available'
                }
            })

    json_schema = {
        'type': 'object',
        'additionalProperties': False,
        'properties': json_props
    }

    return json_schema, metadata, pks

def discover(service):
    catalog = Catalog([])

    for entity_name, entity in service.entities.items():
        optionset_map = get_optionset_metadata(service, entity_name)
        schema_dict, metadata, pks = get_schema(entity.__odata_schema__, optionset_map)
        schema = Schema.from_dict(schema_dict)

        catalog.streams.append(CatalogEntry(
            stream=entity_name,
            tap_stream_id=entity_name,
            key_properties=pks,
            schema=schema,
            metadata=metadata
        ))

    return catalog
