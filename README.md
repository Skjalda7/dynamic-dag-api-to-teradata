# Dynamic DAG
## Objective 📝
Updating database built in Teradata collecting new data from API requests ([AFIP](https://afip.tangofactura.com/Rest/))

## Functionality ⚙️
The columns from the table to update are 'CUIT', 'SITUACION_JURIDICA', 'ESTADO', 'NOMBRE_COMPLETO', 'PROVINCIA', 'LOCALIDAD', 'CODIGO_POSTAL', 'DIRECCION', 'FUENTE', 'FECHA_ACTUALIZACION'.

* From database, extracts a list of 'CUIT' numbers with [generador_cuit](https://github.com/Skjalda7/dynamic-dag-api-to-teradata/blob/main/update_funcional%20-%20GH.py#:~:text=def%20generador_cuit()%3A) function. 
* From the list, the [consulta_afip](https://github.com/Skjalda7/dynamic-dag-api-to-teradata/blob/main/update_funcional%20-%20GH.py#:~:text=def%20consulta_afip(listas_cuits)) function creates a queue with the corresponding request concatenating the URL and the CUIT number so it can make the API call, then the result (json) is stored in a new list. This function is a dynamic task because it decompress the amount of time that requires every request.
* Finally, the [update_teradata](https://github.com/Skjalda7/dynamic-dag-api-to-teradata/blob/main/update_funcional%20-%20GH.py#:~:text=def%20update_teradata(lista)) function filters and processes every single json from the list saving the useful data and updates the table with them executing an 'UPDATE' query.
