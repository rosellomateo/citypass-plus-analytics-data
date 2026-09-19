import azure.functions as func

from bp_data_ingestion_bronze import bp as bp_data_ingestion_bronze
from bp_reclamos_silver import bp as bp_reclamos_silver
from bp_reclamos_gold import bp as bp_reclamos_gold
from bp_movilidad_silver import bp as bp_movilidad_silver
from bp_movilidad_gold import bp as bp_movilidad_gold

app = func.FunctionApp()
app.register_functions(bp_data_ingestion_bronze)
app.register_functions(bp_reclamos_silver)
app.register_functions(bp_reclamos_gold)
app.register_functions(bp_movilidad_silver)
app.register_functions(bp_movilidad_gold)
