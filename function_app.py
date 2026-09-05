import azure.functions as func

from bp_data_ingestion_bronze import bp

app = func.FunctionApp()
app.register_functions(bp)
