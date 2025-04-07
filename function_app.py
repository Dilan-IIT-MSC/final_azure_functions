import azure.functions as func
from bp_user import bp_user
from bp_category import bp_category

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)
app.register_functions(bp_user) 
app.register_functions(bp_category) 