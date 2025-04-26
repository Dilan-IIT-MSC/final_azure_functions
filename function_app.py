import azure.functions as func
from bp_user import bp_user
from bp_category import bp_category
from bp_story import bp_story
from bp_dashboard import bp_dashboard

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)
app.register_functions(bp_user) 
app.register_functions(bp_category)
app.register_functions(bp_story) 
app.register_functions(bp_dashboard) 