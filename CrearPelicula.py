import boto3
import uuid
import os
import json # <-- 1. Importar la librería json

def lambda_handler(event, context):

    try:
        # --- 2. INICIO DEL BLOQUE TRY ---
        # Todo el código original "exitoso" va aquí.
        
        # Entrada (json)
        # print(event) # Se elimina el print(event) original
        tenant_id = event['body']['tenant_id']
        pelicula_datos = event['body']['pelicula_datos'] # <-- Esta línea fallará con el pelicula_07_error.json
        nombre_tabla = os.environ["TABLE_NAME"]
        
        # Proceso
        uuidv4 = str(uuid.uuid4())
        pelicula = {
            'tenant_id': tenant_id,
            'uuid': uuidv4,
            'pelicula_datos': pelicula_datos
        }
        
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(nombre_tabla)
        response = table.put_item(Item=pelicula)
        
        # Salida (json) - Log Estandarizado "INFO"
        # --- 3. REEMPLAZAR EL PRINT() DE ÉXITO ---
        log_info = {
            "tipo": "INFO",
            "log_datos": pelicula
        }
        print(json.dumps(log_info)) # Imprime el log INFO como un string JSON
        
        return {
            'statusCode': 200,
            'pelicula': pelicula,
            'response': response
        }
        # --- FIN DEL BLOQUE TRY ---

    except Exception as e:
        # --- 4. INICIO DEL BLOQUE EXCEPT ---
        # Se captura cualquier error (como el KeyError de "pelicula_patos")
        
        # Log Estandarizado "ERROR"
        log_error = {
            "tipo": "ERROR",
            "log_datos": {
                "mensaje_error": str(e),
                "evento_recibido": event # Logueamos el evento que causó el error
            }
        }
        print(json.dumps(log_error)) # Imprime el log ERROR como un string JSON
        
        # Respuesta de error al cliente (API Gateway)
        return {
            'statusCode': 500, # Internal Server Error
            'body': json.dumps({
                'error': 'Error en el procesamiento de la pelicula', 
                'detalle': str(e)
            })
        }
        # --- FIN DEL BLOQUE EXCEPT ---
