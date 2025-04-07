import azure.functions as func
from bp_user import bp_user
import json
import logging

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)
app.register_functions(bp_user) 

@app.route(route="user/{id}", methods=["GET"])
@app.sql_input(
    arg_name="userRow",
    command_text="SELECT * FROM dbo.Users WHERE id = @id",
    command_type="Text",
    parameters="@id={id}",
    connection_string_setting="SqlConnectionString"
)

def get_user_by_id(req: func.HttpRequest, userRow) -> func.HttpResponse:
    if not userRow:
        return func.HttpResponse("User not found", status_code=404)

    try:
        user = userRow[0]
        user_data = {
            "id": user["id"],
            "firstName": user["firstName"],
            "lastName": user["lastName"]
        }

        return func.HttpResponse(
            body=json.dumps(user_data),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        logging.error(f"Exception while processing userRow: {e}")
        return func.HttpResponse("Internal Server Error", status_code=500)