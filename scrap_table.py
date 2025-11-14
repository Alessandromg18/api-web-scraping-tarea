import requests
from bs4 import BeautifulSoup
import boto3
import uuid

def lambda_handler(event, context):
    
    # URL de la página web del IGP
    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"

    # Realizar la solicitud HTTP
    response = requests.get(url)
    if response.status_code != 200:
        return {
            'statusCode': response.status_code,
            'body': 'Error al acceder a la página web'
        }

    # Parsear HTML
    soup = BeautifulSoup(response.content, 'html.parser')

    # Encontrar la tabla (según inspección real)
    table = soup.find('table', id='tabla_sismos')
    if not table:
        return {
            'statusCode': 404,
            'body': 'No se encontró la tabla en la página web'
        }

    # Extraer encabezados
    headers = [header.text.strip() for header in table.find_all('th')]

    # Extraer las filas
    rows = []
    all_rows = table.find('tbody').find_all('tr')

    # Solo los 10 primeros sismos
    all_rows = all_rows[:10]

    for row in all_rows:
        cells = row.find_all('td')
        rows.append({
            headers[i]: cells[i].text.strip() for i in range(len(cells))
        })

    # Guardar en DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table_db = dynamodb.Table('TablaWebScrapping')

    # Borrar datos anteriores
    scan = table_db.scan()
    with table_db.batch_writer() as batch:
        for each in scan['Items']:
            batch.delete_item(Key={'id': each['id']})

    # Insertar nuevos datos
    i = 1
    for row in rows:
        row['#'] = i
        row['id'] = str(uuid.uuid4())
        table_db.put_item(Item=row)
        i += 1

    return {
        'statusCode': 200,
        'body': rows
    }
