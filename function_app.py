import azure.functions as func

from bronze.bp_data_ingestion_bronze import bp as bp_data_ingestion_bronze
from reclamos.bp_reclamos_silver import bp as bp_reclamos_silver
from reclamos.bp_reclamos_gold import bp as bp_reclamos_gold
from movilidad.bp_movilidad_silver import bp as bp_movilidad_silver
from movilidad.bp_movilidad_gold import bp as bp_movilidad_gold
from residuos.bp_residuos_silver import bp as bp_residuos_silver
from residuos.bp_residuos_gold import bp as bp_residuos_gold
from espacios.bp_espacios_silver import bp as bp_espacios_silver
from espacios.bp_espacios_gold import bp as bp_espacios_gold
from emergencias.bp_emergencias_silver import bp as bp_emergencias_silver
from emergencias.bp_emergencias_gold import bp as bp_emergencias_gold

app = func.FunctionApp()
app.register_functions(bp_data_ingestion_bronze)
app.register_functions(bp_reclamos_silver)
app.register_functions(bp_reclamos_gold)
app.register_functions(bp_movilidad_silver)
app.register_functions(bp_movilidad_gold)
app.register_functions(bp_residuos_silver)
app.register_functions(bp_residuos_gold)
app.register_functions(bp_espacios_silver)
app.register_functions(bp_espacios_gold)
app.register_functions(bp_emergencias_silver)
app.register_functions(bp_emergencias_gold)
