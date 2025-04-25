# Register this blueprint by adding the following line of code 
# to your entry point file.  
# app.register_functions(bp_user) 
# 
# Please refer to https://aka.ms/azure-functions-python-blueprints


import azure.functions as func
import logging
import json
import pyodbc
import os
from datetime import datetime

bp_user = func.Blueprint()

# Helpers
def format_date(date_obj):
    if date_obj:
        return date_obj.strftime('%Y-%m-%d')
    return None

def format_user(user, column_names):
    user_data = {}
    for i, column in enumerate(column_names):
        if column == 'bday' and user[i]:
            user_data[column] = format_date(user[i])
        else:
            user_data[column] = user[i]
    return user_data

#Get User by ID
@bp_user.route(route="user/{id}", methods=["GET"])
def get_user(req: func.HttpRequest) -> func.HttpResponse:
    try:
        user_id = req.route_params.get('id')
        
        try:
            user_id = int(user_id)
        except ValueError:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "Invalid user ID format"
                }),
                mimetype="application/json",
                status_code=200
            )
        
        conn = pyodbc.connect(os.environ["SqlConnectionString"])
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM "user" WHERE id = ?', user_id)
        user = cursor.fetchone()
        
        if not user:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "User not found"
                }),
                mimetype="application/json",
                status_code=200
            )
        
        column_names = [column[0] for column in cursor.description]
        user_data = format_user(user, column_names)
        
        return func.HttpResponse(
            body=json.dumps({
                "status": True,
                "message": "User fetched successfully",
                "user": user_data
            }, default=str),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        logging.error(f"Exception while getting user: {str(e)}")
        return func.HttpResponse(
            body=json.dumps({
                "status": False,
                "message": f"Internal server error: {str(e)}"
            }),
            mimetype="application/json",
            status_code=200
        )
    finally:
        if 'conn' in locals():
            conn.close()

#Get User by Email
@bp_user.route(route="user/email/{email}", methods=["GET"])
def get_user_by_email(req: func.HttpRequest) -> func.HttpResponse:
    try:
        email = req.route_params.get('email')
        
        if not email:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "Email is required"
                }),
                mimetype="application/json",
                status_code=200
            )
        
        conn = pyodbc.connect(os.environ["SqlConnectionString"])
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM "user" WHERE email = ?', email)
        user = cursor.fetchone()
        
        if not user:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "User not found"
                }),
                mimetype="application/json",
                status_code=200
            )
        
        column_names = [column[0] for column in cursor.description]
        user_data = format_user(user, column_names)
        
        return func.HttpResponse(
            body=json.dumps({
                "status": True,
                "message": "User fetched successfully",
                "user": user_data
            }, default=str),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        logging.error(f"Exception while getting user by email: {str(e)}")
        return func.HttpResponse(
            body=json.dumps({
                "status": False,
                "message": f"Internal server error: {str(e)}"
            }),
            mimetype="application/json",
            status_code=200
        )
    finally:
        if 'conn' in locals():
            conn.close()

#Create User
@bp_user.route(route="user", methods=["POST"])
def create_user(req: func.HttpRequest) -> func.HttpResponse:
    try:
        try:
            req_body = req.get_json()
        except ValueError:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "Invalid JSON in request body"
                }),
                mimetype="application/json",
                status_code=200
            )
        
        # Validate required fields
        required_fields = ['firstName', 'email', 'externalId']
        for field in required_fields:
            if field not in req_body:
                return func.HttpResponse(
                    body=json.dumps({
                        "status": False,
                        "message": f"Missing required field: {field}"
                    }),
                    mimetype="application/json",
                    status_code=200
                )
        
        # Validate firstName
        if not isinstance(req_body['firstName'], str):
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "firstName must be a string"
                }),
                mimetype="application/json",
                status_code=200
            )
        
        # Validate email
        if not isinstance(req_body['email'], str):
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "email must be a string"
                }),
                mimetype="application/json",
                status_code=200
            )
        
        # Validate externalId
        if not isinstance(req_body['externalId'], str):
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "externalId must be a string"
                }),
                mimetype="application/json",
                status_code=200
            )
            
        # Validate lastName if provided
        if 'lastName' in req_body and req_body['lastName'] is not None:
            if not isinstance(req_body['lastName'], str):
                return func.HttpResponse(
                    body=json.dumps({
                        "status": False,
                        "message": "lastName must be a string or null"
                    }),
                    mimetype="application/json",
                    status_code=200
                )
        
        # Process birthday if provided
        date_for_sql = None
        if 'bday' in req_body and req_body['bday'] is not None:
            try:
                if not isinstance(req_body['bday'], str):
                    return func.HttpResponse(
                        body=json.dumps({
                            "status": False,
                            "message": "bday must be a string date in format YYYY-MM-DD or null"
                        }),
                        mimetype="application/json",
                        status_code=200
                    )
                
                try:
                    date_parsed = datetime.strptime(req_body['bday'], '%Y-%m-%d')
                    date_for_sql = date_parsed.strftime('%Y-%m-%d')
                except ValueError:
                    return func.HttpResponse(
                        body=json.dumps({
                            "status": False,
                            "message": "bday must be in format YYYY-MM-DD"
                        }),
                        mimetype="application/json",
                        status_code=200
                    )
            except Exception as e:
                logging.error(f"Date conversion error: {e}")
                return func.HttpResponse(
                    body=json.dumps({
                        "status": False,
                        "message": "Invalid date format for bday. Use YYYY-MM-DD"
                    }),
                    mimetype="application/json",
                    status_code=200
                )
        
        conn = pyodbc.connect(os.environ["SqlConnectionString"])
        cursor = conn.cursor()
        
        # Check if email already exists
        cursor.execute('SELECT id FROM "user" WHERE email = ?', req_body['email'])
        existing_email = cursor.fetchone()
        if existing_email:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "Email already exists"
                }),
                mimetype="application/json",
                status_code=200
            )
        
        # Check if externalId already exists
        cursor.execute('SELECT id FROM "user" WHERE externalId = ?', req_body['externalId'])
        existing_externalId = cursor.fetchone()
        if existing_externalId:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "User with this external ID already exists"
                }),
                mimetype="application/json",
                status_code=200
            )
        
        lastName = req_body.get('lastName')
        email = req_body['email']
        externalId = req_body['externalId']
        current_time = datetime.now()
        
        insert_query = '''
            INSERT INTO "user" (
                firstName, lastName, email, externalId, bday, status, created, updated
            ) VALUES (?, ?, ?, ?, ?, 1, ?, ?)
        '''
        cursor.execute(insert_query, 
                      req_body['firstName'], 
                      lastName, 
                      email, 
                      externalId,
                      date_for_sql,
                      current_time,
                      current_time)
        
        cursor.execute("SELECT @@IDENTITY AS id")
        new_id = cursor.fetchone()[0]
        conn.commit()
        
        cursor.execute('SELECT * FROM "user" WHERE id = ?', new_id)
        new_user = cursor.fetchone()
        
        column_names = [column[0] for column in cursor.description]
        user_data = format_user(new_user, column_names)
        
        return func.HttpResponse(
            body=json.dumps({
                "status": True,
                "message": "User created successfully",
                "user": user_data
            }, default=str),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        logging.error(f"Exception while creating user: {str(e)}")
        return func.HttpResponse(
            body=json.dumps({
                "status": False,
                "message": f"Internal server error: {str(e)}"
            }),
            mimetype="application/json",
            status_code=200
        )
    finally:
        if 'conn' in locals():
            conn.close()

#Update User            
@bp_user.route(route="user/{id}", methods=["PUT"])
def update_user(req: func.HttpRequest) -> func.HttpResponse:
    try:
        user_id = req.route_params.get('id')
        try:
            user_id = int(user_id)
        except ValueError:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "Invalid user ID format"
                }),
                mimetype="application/json",
                status_code=200
            )
            
        try:
            req_body = req.get_json()
        except ValueError:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "Invalid JSON in request body"
                }),
                mimetype="application/json",
                status_code=200
            )
        
        valid_fields = ['firstName', 'lastName', 'bday', 'email', 'externalId']
        update_fields = [field for field in valid_fields if field in req_body]
        
        if not update_fields:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "Please provide at least one field to update"
                }),
                mimetype="application/json",
                status_code=200
            )
        
        conn = pyodbc.connect(os.environ["SqlConnectionString"])
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM "user" WHERE id = ?', user_id)
        user = cursor.fetchone()
        if not user:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "User not found"
                }),
                mimetype="application/json",
                status_code=200
            )
        
        update_parts = []
        parameters = []
        
        if 'firstName' in req_body:
            if not isinstance(req_body['firstName'], str):
                return func.HttpResponse(
                    body=json.dumps({
                        "status": False,
                        "message": "firstName must be a string"
                    }),
                    mimetype="application/json",
                    status_code=200
                )
            update_parts.append("firstName = ?")
            parameters.append(req_body['firstName'])
            
        if 'lastName' in req_body:
            if req_body['lastName'] is not None and not isinstance(req_body['lastName'], str):
                return func.HttpResponse(
                    body=json.dumps({
                        "status": False,
                        "message": "lastName must be a string or null"
                    }),
                    mimetype="application/json",
                    status_code=200
                )
            update_parts.append("lastName = ?")
            parameters.append(req_body['lastName'])
            
        if 'email' in req_body:
            if not isinstance(req_body['email'], str):
                return func.HttpResponse(
                    body=json.dumps({
                        "status": False,
                        "message": "email must be a string"
                    }),
                    mimetype="application/json",
                    status_code=200
                )
            # Check if email is already used by another user
            cursor.execute('SELECT id FROM "user" WHERE email = ? AND id != ?', req_body['email'], user_id)
            existing_email = cursor.fetchone()
            if existing_email:
                return func.HttpResponse(
                    body=json.dumps({
                        "status": False,
                        "message": "Email already in use by another user"
                    }),
                    mimetype="application/json",
                    status_code=200
                )
            update_parts.append("email = ?")
            parameters.append(req_body['email'])
            
        if 'externalId' in req_body:
            if not isinstance(req_body['externalId'], str):
                return func.HttpResponse(
                    body=json.dumps({
                        "status": False,
                        "message": "externalId must be a string"
                    }),
                    mimetype="application/json",
                    status_code=200
                )
            # Check if externalId is already used by another user
            cursor.execute('SELECT id FROM "user" WHERE externalId = ? AND id != ?', req_body['externalId'], user_id)
            existing_externalId = cursor.fetchone()
            if existing_externalId:
                return func.HttpResponse(
                    body=json.dumps({
                        "status": False,
                        "message": "External ID already in use by another user"
                    }),
                    mimetype="application/json",
                    status_code=200
                )
            update_parts.append("externalId = ?")
            parameters.append(req_body['externalId'])
            
        if 'bday' in req_body:
            if req_body['bday'] is None:
                update_parts.append("bday = ?")
                parameters.append(None)
            else:
                try:
                    if not isinstance(req_body['bday'], str):
                        return func.HttpResponse(
                            body=json.dumps({
                                "status": False,
                                "message": "bday must be a string date in format YYYY-MM-DD or null"
                            }),
                            mimetype="application/json",
                            status_code=200
                        )
                    
                    try:
                        date_parsed = datetime.strptime(req_body['bday'], '%Y-%m-%d')
                        date_for_sql = date_parsed.strftime('%Y-%m-%d')
                    except ValueError:
                        return func.HttpResponse(
                            body=json.dumps({
                                "status": False,
                                "message": "bday must be in format YYYY-MM-DD"
                            }),
                            mimetype="application/json",
                            status_code=200
                        )
                        
                    update_parts.append("bday = ?")
                    parameters.append(date_for_sql)
                except Exception as e:
                    logging.error(f"Date conversion error: {e}")
                    return func.HttpResponse(
                        body=json.dumps({
                            "status": False,
                            "message": "Invalid date format for bday. Use YYYY-MM-DD"
                        }),
                        mimetype="application/json",
                        status_code=200
                    )
        
        # Add updated timestamp
        update_parts.append("updated = ?")
        parameters.append(datetime.now())
        
        parameters.append(user_id)
        sql_query = f'UPDATE "user" SET {", ".join(update_parts)} WHERE id = ?'
        logging.info(f"Executing SQL: {sql_query} with params: {parameters}")
        cursor.execute(sql_query, parameters)
        conn.commit()
        
        cursor.execute('SELECT * FROM "user" WHERE id = ?', user_id)
        updated_user = cursor.fetchone()
        
        column_names = [column[0] for column in cursor.description]
        user_data = format_user(updated_user, column_names)
        
        return func.HttpResponse(
            body=json.dumps({
                "status": True,
                "message": "User updated successfully",
                "user": user_data
            }, default=str),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        logging.error(f"Exception while updating user: {str(e)}")
        return func.HttpResponse(
            body=json.dumps({
                "status": False,
                "message": f"Internal server error: {str(e)}"
            }),
            mimetype="application/json",
            status_code=200
        )
    finally:
        if 'conn' in locals():
            conn.close()

#Delete User
@bp_user.route(route="user/{id}", methods=["DELETE"])
def delete_user(req: func.HttpRequest) -> func.HttpResponse:
    try:
        user_id = req.route_params.get('id')
        try:
            user_id = int(user_id)
        except ValueError:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "Invalid user ID format"
                }),
                mimetype="application/json",
                status_code=200
            )
        
        conn = pyodbc.connect(os.environ["SqlConnectionString"])
        cursor = conn.cursor()
        
        cursor.execute('SELECT id, status FROM "user" WHERE id = ?', user_id)
        user = cursor.fetchone()
        
        if not user:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "User not found"
                }),
                mimetype="application/json",
                status_code=200
            )
        
        if user[1] == 0:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "User is already deactivated"
                }),
                mimetype="application/json",
                status_code=200
            )
        
        # Update status to 0 and update timestamp
        cursor.execute('UPDATE "user" SET status = 0, updated = ? WHERE id = ?', datetime.now(), user_id)
        conn.commit()
        
        return func.HttpResponse(
            body=json.dumps({
                "status": True,
                "message": "User successfully deactivated",
                "userId": user_id
            }),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        logging.error(f"Exception while deleting user: {str(e)}")
        return func.HttpResponse(
            body=json.dumps({
                "status": False,
                "message": f"Internal server error: {str(e)}"
            }),
            mimetype="application/json",
            status_code=200
        )
    finally:
        if 'conn' in locals():
            conn.close()
