"""Create a source-controlled Power BI project using Microsoft's public PBIR format.

JSON cannot contain comments, so this generator explains the generated files here.
Run it again with your server if you reuse this project in another Snowflake account.
"""
import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / 'powerbi'

def save(path, value):
    file = ROOT / path
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_text(json.dumps(value, indent=2) + '\n', encoding='utf-8')

def literal(value):
    # Power BI stores formatting values as typed expressions, not plain strings.
    return {'expr': {'Literal': {'Value': value}}}

def projection(name, measure=False):
    kind = 'Measure' if measure else 'Column'
    return {'field': {kind: {'Expression': {'SourceRef': {'Entity': 'Orders'}},
                            'Property': name}},
            'queryRef': f'Orders.{name}', 'nativeQueryRef': name}

def visual(name, kind, x, y, width, height, roles, title):
    # Every visual points at a real model column/measure. There are no mock values.
    save(f'RetailDemo.Report/definition/pages/live/visuals/{name}/visual.json', {
        '$schema': 'https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.1.0/schema.json',
        'name': name,
        'position': {'x': x, 'y': y, 'width': width, 'height': height, 'z': x+y, 'tabOrder': x+y},
        'visual': {
            'visualType': kind,
            'query': {'queryState': {role: {'projections': values} for role, values in roles.items()}},
            'visualContainerObjects': {
                'title': [{'properties': {'show': literal('true'), 'text': literal(f"'{title}'")}}],
                'background': [{'properties': {'show': literal('true'), 'color': {'solid': {'color': literal("'#FFFFFF'")}}}}],
            },
            'drillFilterOtherVisuals': True,
        },
        'annotations': [{'name': 'Learning note', 'value': 'This visual queries the Snowflake-backed Orders model.'}],
    })

def build(server):
    save('RetailDemo.pbip', {
        '$schema': 'https://developer.microsoft.com/json-schemas/fabric/pbip/pbipProperties/1.0.0/schema.json',
        'version': '1.0', 'artifacts': [{'report': {'path': 'RetailDemo.Report'}}],
        'settings': {'enableAutoRecovery': True},
    })
    save('RetailDemo.Report/definition.pbir', {
        '$schema': 'https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json',
        'version': '4.0', 'datasetReference': {'byPath': {'path': '../RetailDemo.SemanticModel'}},
    })
    save('RetailDemo.SemanticModel/definition.pbism', {
        '$schema': 'https://developer.microsoft.com/json-schemas/fabric/item/semanticModel/definitionProperties/1.0.0/schema.json',
        'version': '1.0', 'settings': {},
    })
    # A single direct-query partition keeps the beginner model simple.
    # Power BI stores its authentication separately; this model has no password or key.
    source = [
        'let',
        f'    Source = Snowflake.Databases("{server}", "ELT_WH", [Role="RETAIL_REPORTER", Implementation="2.0"]),',
        '    Database = Source{[Name="RETAIL_DEMO", Kind="Database"]}[Data],',
        '    Schema = Database{[Name="MARTS", Kind="Schema"]}[Data],',
        '    Orders = Schema{[Name="FACT_ORDERS", Kind="Table"]}[Data]',
        'in', '    Orders',
    ]
    columns = [
        ('TRANSACTION_ID','int64'), ('ORDERED_AT','dateTime'), ('ORDER_MINUTE','dateTime'),
        ('STORE','string'), ('PRODUCT','string'), ('CATEGORY','string'),
        ('QUANTITY','int64'), ('UNIT_PRICE','decimal'), ('DISCOUNT_PCT','int64'),
        ('SOURCE','string'), ('GROSS_REVENUE','decimal'), ('DISCOUNT_AMOUNT','decimal'),
        ('NET_REVENUE','decimal'), ('LOADED_AT','dateTime'), ('TRANSFORMED_AT','dateTime'),
    ]
    measures = [
        ('Revenue', 'SUM(Orders[NET_REVENUE])', '$#,0.00'),
        ('Transactions', 'COUNTROWS(Orders)', '#,0'),
        ('Average Basket', 'DIVIDE([Revenue], [Transactions], 0)', '$#,0.00'),
        ('Latest Order', 'MAX(Orders[ORDERED_AT])', 'yyyy-MM-dd HH:mm:ss'),
        ('Latest Order ID', 'MAX(Orders[TRANSACTION_ID])', '#,0'),
    ]
    save('RetailDemo.SemanticModel/model.bim', {
        'name': 'RetailDemo', 'compatibilityLevel': 1600,
        'model': {
            'culture': 'en-US', 'defaultPowerBIDataSourceVersion': 'powerBI_V3',
            'dataAccessOptions': {'legacyRedirects': True, 'returnErrorValuesAsNull': True},
            'tables': [{
                'name': 'Orders',
                'description': 'One purchased product per row, loaded by Spark and incrementally transformed by dbt.',
                'columns': [{'name': name, 'dataType': dtype, 'sourceColumn': name,
                             'summarizeBy': 'none' if dtype in ('string','dateTime') or name.endswith('_ID') else 'sum'}
                            for name, dtype in columns],
                'partitions': [{'name': 'Snowflake Orders', 'mode': 'directQuery',
                                'source': {'type': 'm', 'expression': source}}],
                'measures': [{'name': name, 'expression': expression, 'formatString': fmt,
                              'description': f'{name} is calculated from current Snowflake facts.'}
                             for name, expression, fmt in measures],
            }],
            'annotations': [{'name': 'Learning note', 'value': 'DirectQuery reads Snowflake when visuals refresh; it does not import a static snapshot.'}],
        },
    })
    save('RetailDemo.Report/definition/version.json', {
        '$schema': 'https://developer.microsoft.com/json-schemas/fabric/item/report/definition/versionMetadata/1.0.0/schema.json',
        'version': '4.0.0',
    })
    save('RetailDemo.Report/definition/report.json', {
        '$schema': 'https://developer.microsoft.com/json-schemas/fabric/item/report/definition/report/3.0.0/schema.json',
        # Even when no custom theme is used, PBIR requires this object.
        'themeCollection': {},
        'settings': {'useStylableVisualContainerHeader': True},
    })
    save('RetailDemo.Report/definition/pages/pages.json', {
        '$schema': 'https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.0.0/schema.json',
        'pageOrder': ['live'], 'activePageName': 'live',
    })
    save('RetailDemo.Report/definition/pages/live/page.json', {
        '$schema': 'https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.0.0/schema.json',
        'name': 'live', 'displayName': 'Minute Market · Live Sales',
        'displayOption': 'FitToPage', 'height': 800, 'width': 1280,
        'objects': {'background': [{'properties': {'color': {'solid': {'color': literal("'#F2F5F4'")}}, 'transparency': literal('0D')}}]},
        'annotations': [{'name': 'Learning note', 'value': 'Set Page refresh to Auto page refresh, 1 minute, in Desktop. Keep this page active during the demo.'}],
    })
    for i, title in enumerate(['Revenue','Transactions','Average Basket','Latest Order ID']):
        visual(f'kpi{i}', 'cardVisual', 20+i*315, 20, 295, 150,
               {'Data': [projection(title, True)]}, title)
    visual('salesminute', 'lineChart', 20, 190, 820, 300,
           {'Category': [projection('ORDER_MINUTE')], 'Y': [projection('Revenue', True)]}, 'Revenue by minute (UTC)')
    visual('salesstore', 'clusteredBarChart', 860, 190, 400, 300,
           {'Category': [projection('STORE')], 'Y': [projection('Revenue', True)]}, 'Revenue by store')
    visual('orders', 'tableEx', 20, 510, 1240, 270,
           {'Values': [projection(c) for c in ['TRANSACTION_ID','ORDERED_AT','STORE','PRODUCT','QUANTITY','NET_REVENUE']]},
           'Orders received by the warehouse · times in UTC')
    print(f'Created Power BI project in {ROOT}')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server', required=True)
    build(parser.parse_args().server)
